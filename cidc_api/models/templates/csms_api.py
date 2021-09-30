__all__ = [
    "detect_manifest_changes",
    "insert_manifest_from_json",
    "insert_manifest_into_blob",
    "update_json_with_changes",
]

from collections import defaultdict, OrderedDict
from datetime import date, datetime, time
from re import U
from sqlalchemy.orm.session import Session
from typing import (
    Any,
    Callable,
    Dict,
    Iterator,
    List,
    OrderedDict as OrderedDictType,
    Tuple,
    Type,
    Union,
)

from .file_metadata import Upload
from ..models import TrialMetadata, UploadJobStatus, UploadJobs
from .model_core import (
    cimac_id_to_cimac_participant_id,
    MetadataModel,
    with_default_session,
)
from ...shared.auth import get_current_user
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


class NewManifestError(Exception):
    pass


def _get_upload_type(samples: List[Dict[str, Any]]) -> str:
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
                upload_type.add(f"normal_{sample_manifest_type.split()[0].lower()}_dna")
            elif processed_derivative == "Tumor DNA":
                upload_type.add(f"tumor_{sample_manifest_type.split()[0]}_dna")
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

    assert (
        len(upload_type) == 1
    ), f"Inconsistent value determined for upload_type:{upload_type}"
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


def _extract_details_from_trial(
    trial_id: str, manifest_id: str, samples: List[Dict[str, Any]], *, session: Session
):
    """
    Given a trial, return some key values
    
    Returns
    -------
    str : assay_priority
    str : assay_type

    Exceptions Raised
    -----------------
    - f"No assay_priority defined for manifest_id={manifest_id} for trial {trial_id}"
    - f"No assay_type defined for manifest_id={manifest_id} for trial {trial_id}"
    """
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
    return assay_priority, assay_type


def _convert_samples(
    trial_id: str,
    manifest_id: str,
    samples: List[Dict[str, Any]],
    existing_cimac_ids: List[str] = [],
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

        # differences in keys
        if "fixation_or_stabilization_type" in sample:
            sample["fixation_stabilization_type"] = sample.pop(
                "fixation_or_stabilization_type"
            )

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
    assay_priority, assay_type = _extract_details_from_trial(
        trial_id, manifest_id, samples, session=session
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
    for cimac_participant_id, partic_samples in sample_map.items():
        partic = dict(
            cimac_participant_id=cimac_participant_id,
            participant_id=partic_samples[0]["participant_id"],
            **_get_all_values(
                target=Participant, old=partic_samples[0], drop=["trial_participant_id"]
            ),
        )
        partic["samples"] = [
            _get_all_values(target=Sample, old=sample) for sample in partic_samples
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
    assay_priority, assay_type = _extract_details_from_trial(
        trial_id, manifest_id, samples, session=session
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
    for cimac_participant_id, partic_samples in sample_map.items():
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
                **_get_all_values(target=Participant, old=partic_samples[0]),
            )
            ordered_records[Participant].append(new_partic)
        elif partic is None:
            # we're good to add it!
            partic = [
                partic
                for partic in ordered_records[Participant]
                if partic.cimac_participant_id == cimac_participant_id
            ][0]

        for sample in partic_samples:
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

    # add and validate the data
    # the existence of the correct cohort names are checked here
    errs = insert_record_batch(ordered_records, session=session)
    if len(errs):
        raise Exception("Multiple errors: [" + "\n".join(str(e) for e in errs) + "]")

    # create pseudo-Upload
    ordered_records[Upload] = [
        Upload(
            trial_id=trial_id,
            status=UploadJobStatus.MERGE_COMPLETED.value,
            multifile=False,
            assay_creator="DFCI",
            upload_type=_get_upload_type(samples),
            shipment_manifest_id=manifest_id,
            uploader_email=get_current_user().email,
        )
    ]


def _calc_difference(
    cidc: dict,
    csms: dict,
    ignore=[
        "barcode",
        "biobank_id",
        "entry_number",
        "modified_time",
        "modified_timestamp",
        "qc_comments",
        "sample_approved",
        "samples",
        "status",
        "submitter",
    ],
) -> Dict[str, Tuple[Any, Any]]:
    """
    The actual comparison function that handles comparing values

    Handles formatting for date/time/datetime in CIDC
    Do not perform a comparison for ignored keys
    Add constant critical fields back to anything that changes
    """
    # handle formatting and ignore
    cidc1 = {
        k: datetime.strftime(v, "%Y-%m-%d %H:%M:%S")
        if isinstance(v, (date, time, datetime))
        else v
        for k, v in cidc.items()
        if k not in ignore
    }
    csms1 = {
        "trial_id" if k == "protocol_identifier" else k: v
        for k, v in csms.items()
        if k not in ignore
    }

    # take difference by using symmetric set difference on the items
    # use set to not get same key multiple times if values differ
    diff_keys = {k for k, _ in set(cidc1.items()) ^ set(csms1.items())}
    # then get both values once per key to return
    ret = {k: (cidc.get(k), csms.get(k)) for k in diff_keys}

    # add back critical values as labels if there are changes
    # use the new values so we can check them in the source
    if ret:
        for k in ["cimac_id", "manifest_id", "shipment_manifest_id", "trial_id"]:
            if k in csms and k not in ret:
                ret[k] = csms[k]
    return ret


def _get_cidc_sample_map(trial_id, manifest_id, session) -> Dict[str, Dict[str, Any]]:
    """Returns a map of CIMAC IDs for this shipment to the relevant sample details from CIDC"""
    cidc_partic = (
        session.query(Participant).filter(Participant.trial_id == trial_id).all()
    )
    cidc_partic_map = {p.cimac_participant_id: p for p in cidc_partic}
    cidc_samples = (
        session.query(Sample)
        .filter(Sample.shipment_manifest_id == manifest_id, Sample.trial_id == trial_id)
        .all()
    )
    ## make maps from cimac_id to a full dict
    ## need to add participant-level values
    cidc_sample_map = {s.cimac_id: s.to_dict() for s in cidc_samples}
    for cidc_cimac_id in cidc_sample_map.keys():
        cimac_participant_id = cimac_id_to_cimac_participant_id(cidc_cimac_id, {})
        cidc_sample_map[cidc_cimac_id]["cohort_name"] = cidc_partic_map[
            cimac_participant_id
        ].cohort_name
        cidc_sample_map[cidc_cimac_id]["participant_id"] = cidc_partic_map[
            cimac_participant_id
        ].trial_participant_id

    return cidc_sample_map


def _get_csms_sample_map(
    trial_id, manifest_id, csms_samples
) -> Dict[str, Dict[str, Any]]:
    """Returns a map of CIMAC IDs to the relevant sample details from CSMS"""
    return {
        csms_cimac_id: dict(
            # participant-level critical field
            cohort_name=csms_sample["cohort_name"],
            # name changes
            shipment_manifest_id=csms_sample["manifest_id"],
            trial_id=csms_sample["protocol_identifier"],
            participant_id=csms_sample["participant_id"],
            # not in CSMS
            cimac_participant_id=cimac_id_to_cimac_participant_id(csms_cimac_id, {}),
            # the rest of the values
            **_get_all_values(target=Sample, old=csms_sample),
        )
        for csms_cimac_id, csms_sample in _convert_samples(
            trial_id, manifest_id, csms_samples
        )
    }


@with_default_session
def detect_manifest_changes(
    csms_manifest: Dict[str, Any], *, session: Session
) -> Tuple[
    OrderedDictType[Type, List[MetadataModel]],
    Dict[str, List[Dict[str, Union[str, Tuple[Any, Any]]]]],
]:
    """
    Given a CSMS-style manifest, see if it has any differences from the current state of the relational db
    If a new manifest, throws a NewManifestError
    If critical fields are different, throws an error to be handled later by a human
    Returns a list of model instances with changes to any non-critical fields, and the changes themselves

    Returns
    -------
    OrderedDict[Type, List[MetadataModel]]
        instances to pass into insert_record_batch
        contains the changes
    Dict[str, List[Dict[str, Union[str, Tuple[Any, Any]]]]]
        the changes that were detected
        used as input for update_json_with_changes, and aims to be human-readable as well
        ...
        the keys are in ["samples", "shipment", "upload"]
        values are themselves a list of change dicts
            where keys is the field,
            critical values are str, and
            non-critical values are [old, new]
                old or new is None if it was null OR the key was undefined
    
    Raises
    ------
    NewManifestError
        if the manifest_id doesn't correspond to anything in CIDC
    Exception
        if the connections between any critical fields is changed
        namely trial_id, manifest_id, cimac_id
    """
    # Example simple successful returns:
    # {
    #    "upload": [
    #        {
    #            "trial_id": "test_trial",
    #            "manifest_id": "test_manifest",
    #            "upload_type": ("old_type", "new_type")
    #        }
    #    ]
    # }
    # {
    #    "shipment": [
    #        {
    #            "trial_id": "test_trial",
    #            "manifest_id": "test_manifest",
    #            "receiving_party": ("old_party", "new_party")
    #        }
    #    ]
    # }
    # {
    #    "samples": [
    #        {
    #            "trial_id": "test_trial",
    #            "manifest_id": "test_manifest",
    #            "cimac_id": "CTTTPPP00.01",
    #            "participant_id": ("old", "new"), <-- participant level
    #            "sample_location": ( old, new ) <-- sample level
    #        }
    #    ]
    # }
    ret0, ret1 = OrderedDict(), defaultdict(list)

    # ----- Get all our information together -----
    csms_manifest["trial_id"] = csms_manifest.pop("protocol_identifier")
    trial_id, manifest_id, csms_samples = _extract_info_from_manifest(
        csms_manifest, session=session
    )
    csms_assay_priority, csms_assay_type = _extract_details_from_trial(
        trial_id, manifest_id, csms_samples, session=session
    )
    csms_manifest["assay_priority"] = csms_assay_priority
    csms_manifest["assay_type"] = csms_assay_type

    # ----- Look for shipment-level differences -----
    cidc_shipment = (
        session.query(Shipment).filter(Shipment.manifest_id == manifest_id).first()
    )  # Shipment.manifest_id is unique
    if cidc_shipment is None:
        # remove this to allow for adding new manifests via this function
        # also need to uncomment new Sample code below
        raise NewManifestError()
    elif cidc_shipment is not None and Shipment.trial_id != trial_id:
        raise Exception(
            f"Change in critical field for: {(cidc_shipment.trial_id, cidc_shipment.manifest_id)} to CSMS {(trial_id, manifest_id)}"
        )

    diff = _calc_difference(
        {} if cidc_shipment is None else cidc_shipment.to_dict(), csms_manifest
    )
    if diff:
        # since insert_record_batch is a merge, we can just generate a new object
        ret0[Shipment] = [
            Shipment(**_get_all_values(target=Shipment, old=csms_manifest))
        ]
        ret1["shipment"].append(diff)

    # ----- Look for sample-level differences -----
    cidc_sample_map = _get_cidc_sample_map(trial_id, manifest_id, session=session)
    csms_sample_map = _get_csms_sample_map(trial_id, manifest_id, csms_samples)

    # make sure that all of the CIDC samples are still in CSMS
    for cimac_id, cidc_sample in cidc_sample_map.items():
        if cimac_id not in csms_sample_map:
            formatted = (
                cidc_sample["trial_id"],
                cidc_sample["shipment_manifest_id"],
                cidc_sample["cimac_id"],
            )
            raise Exception(
                f"Missing sample: {formatted} on CSMS {(trial_id, manifest_id)}"
            )
    # make sure that all of the CSMS samples are in CIDC
    for cimac_id in csms_sample_map:
        # as sample maps are pulling only from CIDC for this trial_id / manifest_id
        # any missing cimac_id's are a change in critical field
        # but the cimac_id might exist elsewhere in CIDC
        if cimac_id not in cidc_sample_map:
            db_sample = (
                session.query(Sample).filter(Sample.cimac_id == cimac_id).first()
            )  # Sample.cimac_id is unique

            # # uncomment this to allow for adding new manifests to relational
            # # also need to remove NewManifestError thrown above
            # if db_sample is None:
            #     cidc_sample_map[cimac_id] = {}

            formatted = (
                (
                    db_sample.trial_id,
                    db_sample.shipment_manifest_id,
                    db_sample.cimac_id,
                )
                if db_sample is not None
                else f"<no sample found>"
            )
            raise Exception(
                f"Change in critical field for: {formatted} to CSMS {(trial_id, manifest_id, cimac_id)}"
            )

    # add here for ordering and then just append; remove later if not needed
    ret0[Participant] = []
    ret0[Sample] = []
    for cimac_id, csms_sample in csms_sample_map.items():
        diff = _calc_difference(cidc_sample_map[cimac_id], csms_sample)
        if diff:
            ret1["samples"].append(diff)

            # pull out just the changes for ease
            changes = set(diff.keys())
            for i in ["cimac_id", "shipment_manifest_id", "trial_id"]:
                changes.remove(i)

            # Participant-level fields
            if any(
                i in changes
                for i in ["cohort_name", "participant_id", "trial_participant_id"]
            ):
                # since insert_record_batch is a merge, we can just generate a new object
                new_partic = Participant(
                    trial_participant_id=csms_sample["participant_id"],
                    **_get_all_values(target=Participant, old=csms_sample),
                )
                ret0[Participant].append(new_partic)
            for i in ["cohort_name", "participant_id", "trial_participant_id"]:
                if i in changes:
                    changes.remove(i)

            # if there's only cohort_name or trial_participant_id, there's nothing left
            if len(changes):
                # since insert_record_batch is a merge, we can just generate a new object
                new_sample = Sample(**_get_all_values(target=Sample, old=csms_sample))
                ret0[Sample].append(new_sample)

    # ----- Look for differences in the Upload -----
    cidc_upload = (
        session.query(Upload).filter(Upload.shipment_manifest_id == manifest_id).first()
    )
    new_upload = Upload(
        trial_id=trial_id,
        status=UploadJobStatus.MERGE_COMPLETED.value,
        multifile=False,
        assay_creator="DFCI",
        upload_type=_get_upload_type(csms_samples),
        shipment_manifest_id=manifest_id,
        uploader_email=get_current_user().email,
    )
    diff = _calc_difference(
        {} if cidc_upload is None else cidc_upload.to_dict(),
        new_upload.to_dict(),
        ignore=["id", "token", "uploader_email"],
    )
    if diff:
        # since insert_record_batch is a merge, we can just generate a new object
        ret0[Upload] = [new_upload]
        ret1["upload"].append(diff)

    # ----- Finish up and return -----
    # drop unneeded keys
    if len(ret0[Participant]) == 0:
        ret0.pop(Participant)
    if len(ret0[Sample]) == 0:
        ret0.pop(Sample)

    # convert away from defaultdict
    return ret0, dict(ret1)


@with_default_session
def update_json_with_changes(
    trial_id: str,
    all_changes: Dict[str, List[Dict[str, Union[str, Tuple[Any, Any]]]]],
    *,
    session: Session,
) -> List[Exception]:
    """
    Update the trial metadata blobs given a set of changes as returned by detect_manifest_changes
    The form of each input entry is set by the return of _calc_difference
    Uses schemas imports to handle merging
    """
    # schemas import here to keep JSON-blob code together
    from cidc_schemas.prism.merger import PRISM_MERGE_STRATEGIES
    from cidc_schemas.json_validation import load_and_validate_schema
    from jsonmerge import Merger

    errors = []
    changed_sample_map = defaultdict(list)
    shipment_changes, new_upload_type = {}, None

    # Sort through the changes and handle those for manifest itself
    ## just pull out the participant / sample changes for now
    ## upload changes are handled below
    for change_type, change_list in all_changes.items():
        for change in change_list:
            # change = {k: (cidc.get(k), csms.get(k)) for k in <keys with changes>}
            # as well as str values for keys: trial_id, [shipment_]manifest_id, and cimac_id
            change_trial_id, cimac_id = (
                change.pop("trial_id"),
                change.get("cimac_id"),
            )
            manifest_id = (
                change.pop("manifest_id")
                if "manifest_id" in change
                else change.pop("shipment_manifest_id")
            )
            assert (
                change_trial_id == trial_id
            ), f"Tried to use changes for {change_trial_id} to update {trial_id}"

            # these need to be manually updated after merging
            # as shipments
            if change_type == "shipment":
                # only ever one of these
                shipment_changes = {key: new for key, (_, new) in change.items()}

            # this is in a different table than the trial metadata itself, see below
            elif change_type == "upload":
                if "upload_type" in change:
                    new_upload_type = change["upload_type"][1]  # [0] is old value
                continue
            # pull these out to handle separately
            else:  # change_type == "sample"
                cimac_paricipant_id = cimac_id_to_cimac_participant_id(cimac_id, {})
                changed_sample_map[cimac_paricipant_id].append(change)

    # Sort out changes to the participants / samples
    participant_changes = {}  # cimac_participant_id: changes
    for partic_id, sample_list in changed_sample_map.items():
        for sample in sample_list:
            # these are participant-level not sample
            participant_changes[partic_id] = {
                i: sample.pop(i)[1]
                for i in ["cohort_name", "participant_id"]
                if i in sample
            }  # [0] is old value

            cimac_id = sample.pop("cimac_id")
            # if there's only cohort_name or trial_participant_id, there's nothing left
            if len(sample):
                participant_changes[partic_id]["samples"] = {
                    cimac_id: {key: new for key, (_, new) in sample.items()}
                }
                participant_changes[partic_id]["samples"][cimac_id][
                    "cimac_id"
                ] = cimac_id
                participant_changes[partic_id]["samples"][cimac_id][
                    "shipment_manifest_id"
                ] = manifest_id

    # merge new patch into trial and update the db record
    # have to do by hand as using merge_clinical_trial_metadata leads to conflict errors
    new_md = TrialMetadata.select_for_update_by_trial_id(trial_id).metadata_json.copy()
    for partic_id, changes in participant_changes.items():
        samples_to_change = changes.pop("samples", {})

        # it exists, go find it and update it
        if partic_id in [p["cimac_participant_id"] for p in new_md["participants"]]:
            for n, participant in enumerate(new_md["participants"]):
                if participant["cimac_participant_id"] == partic_id:
                    new_md["participants"][n].update(changes)

                    # handle samples for this participant
                    for cimac_id, sample_dict in samples_to_change.items():
                        sample_dict.pop("shipment_manifest_id")
                        # it exists, go find it and update it
                        if cimac_id in [
                            s["cimac_id"] for s in new_md["participants"][n]["samples"]
                        ]:
                            for m, sample in enumerate(
                                new_md["participants"][n]["samples"]
                            ):
                                if sample["cimac_id"] == cimac_id:
                                    new_md["participants"][n]["samples"][m].update(
                                        sample_dict
                                    )
                                    break
                        else:  # just add it to the end
                            new_md["participants"][n]["samples"].append(
                                {key: new for key, (_, new) in sample_dict}
                            )
                    break
        else:  # just add it to the end
            for sample_dict in samples_to_change.values():
                changes.update(sample_dict)
            these_changes = {key: new for key, (_, new) in changes}
            new_partic = dict(
                participant_id=changes["participant_id"],
                **_get_all_values(Participant, these_changes),
            )
            new_md["participants"].append(new_partic)
            new_md["participants"]["samples"] = [
                {
                    key: new
                    for key, (_, new) in sample_dict
                    if key != "shipment_manifest_id"
                }
                for sample_dict in samples_to_change.values()
            ]

    # handle shipment changes
    if shipment_changes:
        for n, shipment in enumerate(new_md["shipments"]):
            if shipment["manifest_id"] == manifest_id:
                new_md["shipments"][n].update(shipment_changes)
                break

    # Update the trial
    try:
        TrialMetadata.select_for_update_by_trial_id(trial_id).update(
            changes={"metadata_json": new_md}
        )
    except Exception as e:
        errors.append(e)

    # UploadJobs are stored in a different table, so we need to update that too
    if not len(errors):
        # go get all the possible uploads
        # we have to find the one to update by manifest_id in the metadata_patch
        uploads = (
            session.query(UploadJobs)
            .filter(
                UploadJobs.trial_id == trial_id,
                UploadJobs.status == UploadJobStatus.MERGE_COMPLETED.value,
            )
            .all()
        )
        for upload in uploads:
            shipments = upload.metadata_patch.get("shipments", [])
            if len(shipments) and shipments[0].get("manifest_id") == manifest_id:
                # merge the new changes into the existing metadata patch
                # and then update the db record
                try:

                    ct_schema = load_and_validate_schema("clinical_trial.json")

                    upload.update(
                        changes={
                            # this is not truly a patch, as it contains all of the trial md
                            "metadata_patch": new_md,
                            "upload_type": new_upload_type
                            if new_upload_type
                            else upload.upload_type,
                        },
                    )
                except Exception as e:
                    errors.append(e)
                finally:  # either way we're done
                    break

    return errors
