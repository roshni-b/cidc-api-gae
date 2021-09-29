__all__ = ["insert_manifest_from_json", "insert_manifest_into_blob"]

from collections import defaultdict, OrderedDict
from sqlalchemy.orm.session import Session
from typing import Any, Callable, Dict, Iterator, List, Tuple, Union

from .file_metadata import Upload
from ..models import TrialMetadata, UploadJobStatus, UploadJobs
from .model_core import cimac_id_to_cimac_participant_id, with_default_session
from .sync_schemas import _get_all_values
from .trial_metadata import (
    cimac_id_regex,
    ClinicalTrial,
    CollectionEvent,
    Participant,
    Sample,
    Shipment,
)
from .utils import insert_record_batch


def _get_upload_type(samples: Dict[str, Any]) -> str:
    upload_type = set()
    for sample in samples:
        processed_type = sample.get("processed_sample_type").lower()
        if processed_type == "h&e-stained fixed tissue slide specimen":
            processed_type = "h_and_e"

        if processed_type in [
            "pbmc",
            "plasma",
            "tissue_slide",
            "normal_blood_dna",
            "normal_tissue_dna",
            "tumor_tissue_dna",
            "tumor_tissue_rna",
            "h_and_e",
        ]:
            upload_type.add(processed_type)
        else:
            sample_manifest_type = sample.get("sample_manifest_type")
            processed_derivative = sample.get("processed_sample_derivative")
            if sample_manifest_type is None:
                # safety
                continue

            elif sample_manifest_type == "biofluid_cellular":
                upload_type.add("pbmc")
            elif sample_manifest_type == "tissue_slides":
                upload_type.add("tissue_slide")

            elif processed_derivative == "Germline DNA":
                upload_type.add(f"normal_{sample_manifest_type.split('_')[0]}_dna")
            elif processed_derivative == "Tumor DNA":
                upload_type.add(f"tumor_{sample_manifest_type.split('_')[0]}_dna")
            elif processed_derivative in ["DNA", "RNA"]:
                unprocessed_type = sample.get("type_of_sample")
                new_type = "tumor" if "tumor" in unprocessed_type.lower() else "normal"
                new_type += (
                    "_blood_"
                    if sample_manifest_type.startswith("biofluid")
                    else "_tissue_"
                )
                new_type += processed_derivative.lower()

                upload_type.add(new_type)

    assert len(upload_type) == 1, "Inconsistent value determined for upload_type"
    return list(upload_type)[0]


def _get_and_check(
    obj: Union[Dict[str, Any], List[Dict[str, Any]]],
    key: str,
    msg: Callable[[Any], str],
    default: Any = None,
    check: Callable[[Any], bool] = bool,
) -> Any:
    """
    Returns a key from a dictionary if it exists, and raises an error if fails an integrity check
    If given a list of dictionaries, asserts that each one provides the same result.
    """
    if isinstance(obj, list):
        ret = {o.get(key, default) for o in obj}
        assert len(ret) == 1, f"Inconsistent value provided for {key}"
        ret = list(ret)[0]
    else:
        ret = obj.get(key, default)

    if not check(ret):
        raise Exception(msg)
    else:
        return ret


def _extract_info_from_manifest(
    manifest: Dict[str, Any], *, session: Session
) -> Tuple[str, str, List[Dict[str, Any]]]:
    """
    Given a manifest, do initial validation and return some key values
    
    Returns
    -------
    str : trial_id
        the same across all samples
        exists in both TrialMetadata and ClinicalTrial tables
    str : manifest_id
    List[Dict[str, Any]] : samples

    Exceptions Raised
    -----------------
    - "Cannot add a manifest that is not qc_complete"
        if manifest's status is not qc_complete (or null)
    - f"Manifest {manifest_id} contains no samples: {manifest}"
    - f"No consistent protocol_identifier defined for samples on manifest {manifest_id}"
    - f"Clinical trial with protocol identifier={trial_id} does not exist"
        if trial is missing from TrialMetadata OR ClinicalTrial OR both
    """
    manifest_id = _get_and_check(
        obj=manifest, key="manifest_id", msg=f"No manifest_id in: {manifest}"
    )
    _ = _get_and_check(  # don't need to keep status
        obj=manifest,
        key="status",
        msg="Cannot add a manifest that is not qc_complete",
        default="qc_complete",
        check=lambda v: v == "qc_complete",
    )
    samples = _get_and_check(
        obj=manifest,
        key="samples",
        msg=f"Manifest {manifest_id} contains no samples: {manifest}",
        default=[],
        check=lambda v: len(v) != 0,
    )
    trial_id = _get_and_check(
        obj=samples,
        key="protocol_identifier",
        msg=f"No consistent protocol_identifier defined for samples on manifest {manifest_id}",
    )

    # Also verify that the trial exists
    trial_md = TrialMetadata.select_for_update_by_trial_id(
        trial_id, session=session
    )  # JSON
    trial = ClinicalTrial.get_by_id(trial_id, session=session)  # relational
    if trial_md is None or trial is None:
        raise Exception(
            f"Clinical trial with protocol identifier={trial_id} does not exist"
        )

    return trial_id, manifest_id, samples


def _convert_samples(
    trial_id: str,
    manifest_id: str,
    samples: List[Dict[str, Any]],
    existing_cimac_ids: List[str],
) -> Iterator[Tuple[str, Dict[str, Any]]]:
    """
    Convert a list of CSMS-style samples into an iterator returning CIMAC IDs and CIDC-style samples
    Exceptions are raised during the call for each sample; full validation is NOT done first.

    Returns
    -------
    iterator yielding (str, dict)
        cimac_id, sample

    Exceptions Raised
    -----------------
    - f"No standardized_collection_event_name defined for sample {sample['cimac_id']} on manifest {sample['manifest_id']} for trial {sample['protocol_identifier']}"
    - f"No cimac_id defined for samples[{n}] on manifest_id={manifest_id} for trial {trial_id}"
    - f"Malformatted cimac_id={cimac_id} on manifest_id={manifest_id} for trial {trial_id}"
    - f"Sample with cimac_id={cimac_id} already exists for trial {trial_id}\nNew samples: {sample}"
    - f"Sample with no local participant_id given:\n{sample}"
        if participant_id and trial_participant_id are both undefined
    """
    for n, sample in enumerate(samples):
        event_name = sample.get("standardized_collection_event_name")
        if event_name is None:
            raise Exception(
                f"No standardized_collection_event_name defined for sample {sample['cimac_id']} on manifest {sample['manifest_id']} for trial {sample['protocol_identifier']}"
            )
        else:
            sample["collection_event_name"] = event_name

        # differences in naming convention
        processed_sample_type_map: Dict[str, str] = {
            "tissue_slide": "Fixed Slide",
            "tumor_tissue_dna": "Tissue Scroll",
            "plasma": "Plasma",
            "normal_tissue_dna": "Tissue Scroll",
            "h_and_e": "H&E-Stained Fixed Tissue Slide Specimen",
        }
        if sample["processed_sample_type"] in processed_sample_type_map:
            sample["processed_sample_type"] = processed_sample_type_map[
                sample["processed_sample_type"]
            ]

        # typing
        if "sample_derivative_concentration" in sample:
            sample["sample_derivative_concentration"] = float(
                sample["sample_derivative_concentration"]
            )

        # "Not Reported" as default when required
        if (
            sample["type_of_sample"] == "Blood"
            and "type_of_primary_container" not in sample
        ):
            sample["type_of_primary_container"] = "Not Reported"
        if "parent_sample_id" not in sample:
            sample["parent_sample_id"] = "Not Reported"

        cimac_id = _get_and_check(
            obj=sample,
            key="cimac_id",
            msg=f"No cimac_id defined for samples[{n}] on manifest_id={manifest_id} for trial {trial_id}",
        )
        if not cimac_id_regex.match(cimac_id):
            raise Exception(
                f"Malformatted cimac_id={cimac_id} on manifest_id={manifest_id} for trial {trial_id}"
            )
        elif cimac_id in existing_cimac_ids:
            raise Exception(
                f"Sample with cimac_id={cimac_id} already exists for trial {trial_id}\nNew samples: {sample}"
            )

        # let's just have both: JSON needs participant_id and relational trial_participant_id
        if "participant_id" in sample:
            sample["trial_participant_id"] = sample["participant_id"]
        elif "trial_participant_id" in sample:
            sample["participant_id"] = sample["trial_participant_id"]
        else:
            raise Exception(f"Sample with no local participant_id given:\n{sample}")

        yield (cimac_id, sample)


@with_default_session
def insert_manifest_into_blob(manifest: Dict[str, Any], *, session: Session):
    """
    Given a CSMS-style manifest, add it into the JSON metadata blob
    
    Exceptions Raised
    -----------------
    - "Cannot add a manifest that is not qc_complete"
        if manifest's status is not qc_complete (or null)
    - f"Manifest {manifest_id} contains no samples: {manifest}"
    - f"No consistent protocol_identifier defined for samples on manifest {manifest_id}"
    - f"Clinical trial with protocol identifier={trial_id} does not exist"
        if trial is missing from TrialMetadata OR ClinicalTrial OR both

    - Assertion: "Inconsistent value provided for assay_priority"
    - Assertion: "Inconsistent value provided for assay_type"

    - f"Manifest with manifest_id={manifest_id} already exists for trial {trial_id}"
    - f"No standardized_collection_event_name defined for sample {sample['cimac_id']} on manifest {sample['manifest_id']} for trial {sample['protocol_identifier']}"
    - f"No cimac_id defined for samples[{n}] on manifest_id={manifest_id} for trial {trial_id}"
    - f"Malformatted cimac_id={cimac_id} on manifest_id={manifest_id} for trial {trial_id}"
    - f"Sample with cimac_id={cimac_id} already exists for trial {trial_id}\nNew samples: {sample}"
    - f"Sample with no local participant_id given:\n{sample}"
        if participant_id and trial_participant_id are both undefined

    - "prism errors: [{errors from merge_clinical_trial_metadata}]"
    """
    # schemas import here to keep JSON-blob code together
    from cidc_schemas.prism.merger import merge_clinical_trial_metadata

    # also need to get current user for pseudo-UploadJobs
    from ...shared.auth import get_current_user

    trial_id, manifest_id, samples = _extract_info_from_manifest(
        manifest, session=session
    )
    trial_md = TrialMetadata.select_for_update_by_trial_id(trial_id)
    if manifest_id in [s["manifest_id"] for s in trial_md.metadata_json["shipments"]]:
        raise Exception(
            f"Manifest with manifest_id={manifest_id} already exists for trial {trial_id}"
        )

    # pull out some additional values we'll need
    existing_cimac_ids = [
        s["cimac_id"]
        for p in trial_md.metadata_json["participants"]
        for s in p["samples"]
    ]
    assay_priority = _get_and_check(
        obj=samples,
        key="assay_priority",
        msg=f"No assay_priority defined for manifest_id={manifest_id} for trial {trial_id}",
        default="Not Reported",
    )
    assay_type = _get_and_check(
        obj=samples,
        key="assay_type",
        msg=f"No assay_type defined for manifest_id={manifest_id} for trial {trial_id}",
    )

    # a patch is just the parts that are new, equivalent to the return of schemas.prismify
    patch = {
        "protocol_identifier": trial_id,
        "shipments": [
            dict(
                assay_priority=assay_priority,
                assay_type=assay_type,
                **_get_all_values(target=Shipment, old=manifest),
            )
        ],
        "participants": [],
    }

    # sort samples by participants
    sample_map: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for cimac_id, sample in _convert_samples(
        trial_id, manifest_id, samples, existing_cimac_ids
    ):
        sample_map[cimac_id_to_cimac_participant_id(cimac_id, {})].append(sample)

    # each participant has a list of samples
    for cimac_participant_id, samples in sample_map.items():
        partic = dict(
            cimac_participant_id=cimac_participant_id,
            participant_id=samples[0]["participant_id"],
            **_get_all_values(
                target=Participant, old=samples[0], drop=["trial_participant_id"]
            ),
        )
        partic["samples"] = [
            _get_all_values(target=Sample, old=sample) for sample in samples
        ]

        patch["participants"].append(partic)

    # merge and validate the data
    # the existence of the correct cohort and collection_event names are checked here
    merged, errs = merge_clinical_trial_metadata(patch, trial_md.metadata_json)
    if len(errs):
        raise Exception({"prism errors": [str(e) for e in errs]})

    # save it
    trial_md.update(changes={"metadata_json": merged})

    # create pseudo-UploadJobs
    UploadJobs(
        trial_id=trial_id,
        _status=UploadJobStatus.MERGE_COMPLETED.value,
        multifile=False,
        metadata_patch=patch,
        upload_type=_get_upload_type(samples),
        uploader_email=get_current_user().email,
    ).insert()


@with_default_session
def insert_manifest_from_json(
    manifest: Dict[str, Any], *, session: Session
) -> List[Exception]:
    """
    Given a CSMS-style manifest, validate and add it into the relational tables.

    Returns errors
    
    Exceptions Raised
    -----------------
    - "Cannot add a manifest that is not qc_complete"
        if manifest's status is not qc_complete (or null)
    - f"Manifest {manifest_id} contains no samples: {manifest}"
    - f"No consistent protocol_identifier defined for samples on manifest {manifest_id}"
    - f"Clinical trial with protocol identifier={trial_id} does not exist"
        if trial is missing from TrialMetadata OR ClinicalTrial OR both

    - Assertion: "Inconsistent value provided for assay_priority"
    - Assertion: "Inconsistent value provided for assay_type"

    - f"Manifest with manifest_id={manifest_id} already exists for trial {trial_id}"
    - f"No standardized_collection_event_name defined for sample {sample['cimac_id']} on manifest {sample['manifest_id']} for trial {sample['protocol_identifier']}"
    - f"No cimac_id defined for samples[{n}] on manifest_id={manifest_id} for trial {trial_id}"
    - f"Malformatted cimac_id={cimac_id} on manifest_id={manifest_id} for trial {trial_id}"
    - f"Sample with cimac_id={cimac_id} already exists for trial {trial_id}\nNew samples: {sample}"
    - f"Sample with no local participant_id given:\n{sample}"
        if participant_id and trial_participant_id are both undefined

    - "No Collection event with trial_id, event_name = {trial_id}, {event_name}; needed for sample {cimac_id} on manifest {manifest_id}"
    - "Multiple errors: [{errors from insert_record_batch}]"
    """
    trial_id, manifest_id, samples = _extract_info_from_manifest(
        manifest, session=session
    )

    if (
        session.query(Shipment)
        .filter(Shipment.manifest_id == manifest_id, Shipment.trial_id == trial_id)
        .first()
        is not None
    ):
        raise Exception(
            f"Manifest with manifest_id={manifest_id} already exists for trial {trial_id}"
        )

    # pull out some additional values we'll need
    existing_cimac_ids = []
    for sample in session.query(Sample).filter(Sample.trial_id == trial_id).all():
        existing_cimac_ids.append(sample.cimac_id)
    assay_priority = _get_and_check(
        obj=samples,
        key="assay_priority",
        msg=f"No assay_priority defined for manifest_id={manifest_id} for trial {trial_id}",
        default="Not Reported",
    )
    assay_type = _get_and_check(
        obj=samples,
        key="assay_type",
        msg=f"No assay_type defined for manifest_id={manifest_id} for trial {trial_id}",
    )

    # need to insert Shipment and Participants before Samples
    ordered_records = OrderedDict()
    ordered_records[Shipment] = [
        Shipment(
            trial_id=trial_id,
            assay_priority=assay_priority,
            assay_type=assay_type,
            **_get_all_values(target=Shipment, old=manifest),
        )
    ]

    # sort samples by participants
    sample_map: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for cimac_id, sample in _convert_samples(
        trial_id, manifest_id, samples, existing_cimac_ids
    ):
        sample_map[cimac_id_to_cimac_participant_id(cimac_id, None)].append(sample)

    # each participant has a list of samples
    ordered_records[Participant] = []
    ordered_records[Sample] = []
    for cimac_participant_id, samples in sample_map.items():
        # add the participant if they don't already exist
        partic = Participant.get_by_id(trial_id, cimac_participant_id)
        if partic is None and not any(
            # check if we're already going to add it
            [
                p.cimac_participant_id == cimac_participant_id
                for p in ordered_records[Participant]
            ]
        ):
            new_partic = Participant(
                trial_id=trial_id,
                cimac_participant_id=cimac_participant_id,
                **_get_all_values(target=Participant, old=sample),
            )
            ordered_records[Participant].append(new_partic)
        elif partic is None:
            # we're good to add it!
            partic = [
                partic
                for partic in ordered_records[Participant]
                if partic.cimac_participant_id == cimac_participant_id
            ][0]

        for sample in samples:
            # explicitly make sure the CollectionEvent exists
            # as the foreign key is nullable, but should be defined
            event_name = sample["collection_event_name"]
            if (
                session.query(CollectionEvent)
                .filter(
                    CollectionEvent.trial_id == trial_id,
                    CollectionEvent.event_name == event_name,
                )
                .first()
                is None
            ):
                raise Exception(
                    f"No Collection event with trial_id, event_name = {trial_id}, {event_name}; needed for sample {cimac_id} on manifest {manifest_id}"
                )

            new_sample = Sample(
                trial_id=trial_id,
                cimac_participant_id=cimac_id_to_cimac_participant_id(cimac_id, {}),
                **_get_all_values(target=Sample, old=sample),
            )
            ordered_records[Sample].append(new_sample)

    # create pseudo-Upload
    ordered_records[Upload] = [
        Upload(
            trial_id=trial_id,
            status=UploadJobStatus.MERGE_COMPLETED.value,
            multifile=False,
            assay_creator="DFCI",
            upload_type=_get_upload_type(samples),
        )
    ]

    # add and validate the data
    # the existence of the correct cohort names are checked here
    errs = insert_record_batch(ordered_records, session=session)
    if len(errs):
        raise Exception("Multiple errors: [" + "\n".join(str(e) for e in errs) + "]")
