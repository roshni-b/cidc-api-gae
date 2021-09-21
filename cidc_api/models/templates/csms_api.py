__all__ = [
    "insert_manifest_from_json",
    "insert_manifest_into_blob",
]

from collections import defaultdict, OrderedDict
from sqlalchemy.orm.session import Session
from typing import Any, Callable, Dict, List, Tuple

from ..models import TrialMetadata
from .model_core import cimac_id_to_cimac_participant_id, with_default_session
from .sync_schemas import _get_all_values
from .trial_metadata import (
    cimac_id_regex,
    ClinicalTrial,
    Cohort,
    CollectionEvent,
    Participant,
    Sample,
    Shipment,
)
from .utils import insert_record_batch


def _convert_sample(sample: Dict[str, Any]) -> Dict[str, Any]:
    event_name = sample.get("standardized_collection_event_name")
    if event_name is None:
        raise Exception(
            f"No standardized_collection_event_name defined for sample {sample['cimac_id']} on manifest {sample['manifest_id']} for trial {sample['protocol_identifier']}"
        )
    else:
        sample["collection_event_name"] = event_name

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

    if "sample_derivative_concentration" in sample:
        sample["sample_derivative_concentration"] = float(
            sample["sample_derivative_concentration"]
        )

    if (
        sample["type_of_sample"] == "Blood"
        and "type_of_primary_container" not in sample
    ):
        sample["type_of_primary_container"] = "Not Reported"

    if "parent_sample_id" not in sample:
        sample["parent_sample_id"] = "Not Reported"

    return sample


def _get_and_check(
    obj: Dict[str, Any],
    key: str,
    msg: Callable[[Any], str],
    default: Any = None,
    check: Callable[[Any], bool] = bool,
) -> Any:
    """Returns a key from a dictionary if it exists, and raises an error if fails an integrity check"""
    ret = obj.get(key, default)
    if not check(ret):
        raise Exception(msg)
    else:
        return ret


def _get_and_check_trial(
    manifest_id: str, samples: List[Dict[str, Any]], *, session: Session
) -> Tuple[str, List[str]]:
    """Given a manifest, returns the trial_id and the list of existing cimac_id's"""
    trial_id = {
        _get_and_check(
            obj=sample,
            key="protocol_identifier",
            msg=f"No protocol_identifier defined for samples[{n}] on manifest {manifest_id}:\n{sample}",
        )
        for n, sample in enumerate(samples)
    }
    if len(trial_id) != 1:
        raise NotImplementedError(f"Multiple trials on a single shipment: {trial_id}")
    trial_id = list(trial_id)[0]
    trial = ClinicalTrial.get_by_id(trial_id, session=session)
    if not trial:
        raise Exception(
            f"Clinical trial with protocol identifier={trial_id} does not exist"
        )
    return trial_id


@with_default_session
def insert_manifest_into_blob(manifest: Dict[str, Any], *, session: Session):
    from cidc_schemas.prism.merger import merge_clinical_trial_metadata

    manifest_id = _get_and_check(
        obj=manifest, key="manifest_id", msg=f"No manifest_id in: {manifest}",
    )
    _get_and_check(
        obj=manifest,
        key="status",
        msg=f"Cannot add manifest {manifest_id} that is not qc_complete: {manifest}",
        default="qc_complete",
        check=lambda v: v == "qc_complete",
    )
    samples: List[Dict[str, Any]] = _get_and_check(
        obj=manifest,
        key="samples",
        msg=f"Manifest {manifest_id} contains no samples: {manifest}",
        default=[],
        check=lambda v: len(v) != 0,
    )

    trial_id = _get_and_check_trial(
        manifest_id, manifest.get("samples", []), session=session
    )
    trial_md = TrialMetadata.select_for_update_by_trial_id(trial_id)
    existing_cimac_ids = [
        s["cimac_id"]
        for p in trial_md.metadata_json["participants"]
        for s in p["samples"]
    ]

    assay_priority = {s.get("assay_priority", "Not Reported") for s in samples}
    assert len(assay_priority) == 1
    assay_priority = list(assay_priority)[0]

    assay_type = {s.get("assay_type") for s in samples}
    assert len(assay_type) == 1
    assay_type = list(assay_type)[0]
    assert assay_type is not None, str(samples)

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
    if patch["shipments"][0]["manifest_id"] in [
        s["manifest_id"] for s in trial_md.metadata_json["shipments"]
    ]:
        raise Exception(
            f"Manifest with manifest_id={patch['shipments'][0]['manifest_id']} already exists for trial {trial_id}"
        )

    sample_map: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for n, sample in enumerate(samples):
        sample = _convert_sample(sample)
        if "trial_participant_id" in sample:
            sample["participant_id"] = sample["trial_participant_id"]
        elif "participant_id" not in sample:
            raise Exception(f"Sample with no local participant_id given:\n{sample}")

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

        sample_map[cimac_id_to_cimac_participant_id(cimac_id, None)].append(sample)

    for cimac_participant_id, samples in sample_map.items():
        # just to make sure doesn't fail later
        if not len(samples):
            continue

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

    merged, errs = merge_clinical_trial_metadata(patch, trial_md.metadata_json)
    if len(errs):
        raise Exception({"prism errors": [str(e) for e in errs]})

    trial_md.update(changes={"metadata_json": merged})


@with_default_session
def insert_manifest_from_json(
    manifest: Dict[str, Any], *, session: Session
) -> List[Exception]:
    """
    For use in conjunction with CSMS to create new samples from a given manifest.
    """
    manifest_id = _get_and_check(
        obj=manifest, key="manifest_id", msg=f"No manifest_id in: {manifest}",
    )
    _get_and_check(
        obj=manifest,
        key="status",
        msg=f"Cannot add a manifest that is not qc_complete",
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
    trial_id = _get_and_check_trial(
        manifest_id, manifest.get("samples", []), session=session
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

    existing_cimac_ids = []
    for sample in session.query(Sample).filter(Sample.trial_id == trial_id).all():
        existing_cimac_ids.append(sample.cimac_id)

    assay_priority = {s.get("assay_priority", "Not Reported") for s in samples}
    assert len(assay_priority) == 1
    assay_priority = list(assay_priority)[0]

    assay_type = {s.get("assay_type") for s in samples}
    assert len(assay_type) == 1
    assay_type = list(assay_type)[0]
    assert assay_type is not None

    ordered_records = OrderedDict()
    ordered_records[Shipment] = [
        Shipment(
            trial_id=trial_id,
            assay_priority=assay_priority,
            assay_type=assay_type,
            **_get_all_values(target=Shipment, old=manifest),
        )
    ]
    ordered_records[Participant] = []
    ordered_records[Sample] = []
    for n, sample in enumerate(samples):
        sample = _convert_sample(sample)
        if "participant_id" in sample:
            sample["trial_participant_id"] = sample.pop("participant_id")

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

        cimac_participant_id = cimac_id_to_cimac_participant_id(cimac_id, None)
        partic = Participant.get_by_id(trial_id, cimac_participant_id)
        if partic is None and not any(
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
        else:
            if partic is None:
                partic = [
                    partic
                    for partic in ordered_records[Participant]
                    if partic.cimac_participant_id == cimac_participant_id
                ][0]

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
            cimac_participant_id=cimac_id_to_cimac_participant_id(cimac_id, None),
            **_get_all_values(target=Sample, old=sample),
        )
        ordered_records[Sample].append(new_sample)

    errs = insert_record_batch(ordered_records, session=session)
    if len(errs):
        raise Exception("Multiple errors: [" + "\n".join(str(e) for e in errs) + "]")
