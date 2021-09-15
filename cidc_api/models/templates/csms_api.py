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


def _get_and_check(
    obj: Dict[str, Any],
    key: str,
    msg: Callable[[Any], str],
    default: Any = None,
    check: Callable[[Any], bool] = bool,
) -> Any:
    """Returns a key from a dictionary if it exists, and raises an error if fails an integrity check"""
    ret = obj.get(key, default)
    if not check(key):
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
    if any([shipment.manifest_id == manifest_id for shipment in trial.shipments]):
        raise Exception(
            f"Manifest with manifest_id={manifest_id} already exists for trial {trial_id}"
        )
    existing_cimac_ids = []
    for partic in trial.participants:
        for sample in partic.samples:
            existing_cimac_ids.append(sample.cimac_id)

    return trial_id, existing_cimac_ids


def _need_to_insert(obj: Any, existing: List[Any], key: str) -> bool:
    """If obj is not None and not in existing when matched by keep, we need to add it!"""
    if obj is None:
        return True
    else:
        return getattr(obj, key) in [getattr(x, key) for x in existing]


@with_default_session
def insert_manifest_into_blob(manifest: Dict[str, Any], *, session: Session):
    from cidc_schemas.prism.merger import merge_clinical_trial_metadata

    manifest_id = _get_and_check(
        obj=manifest, key="manifest_id", msg=f"No manifest_id in: {manifest}",
    )
    samples: List[Dict[str, Any]] = _get_and_check(
        obj=manifest,
        key="samples",
        msg=f"Manifest {manifest_id} contains no samples: {manifest}",
        default=[],
        check=lambda v: len(v) != 0,
    )

    trial_id, existing_cimac_ids = _get_and_check_trial(
        manifest.get("samples", []), session=session
    )
    trial_md = TrialMetadata.select_for_update_by_trial_id(trial_id)

    sample_map: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for sample in samples:
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
        else:
            sample["trial_participant_id"] = sample.pop("participant_id", None)

        sample_map[cimac_id_to_cimac_participant_id(cimac_id)].append(sample)

    patch = {
        "protocol_identifier": trial_id,
        "shipments": [_get_all_values(target=Shipment, old=manifest)],
        "participants": {},
    }
    for cimac_participant_id, samples in enumerate(sample_map.items()):
        # just to make sure doesn't fail later
        if not len(samples):
            continue

        partic = dict(
            cimac_participant_id=cimac_participant_id
            ** _get_all_values(target=Participant, old=samples[0])
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
    samples = _get_and_check(
        obj=manifest,
        key="samples",
        msg=f"Manifest {manifest_id} contains no samples: {manifest}",
        default=[],
        check=lambda v: len(v) != 0,
    )
    trial_id, existing_cimac_ids = _get_and_check_trial(
        manifest.get("samples", []), session=session
    )

    ordered_records = OrderedDict()
    ordered_records[Shipment] = [
        Shipment(trial_id=trial_id, **_get_all_values(target=Shipment, old=manifest))
    ]
    ordered_records[Cohort] = []
    ordered_records[CollectionEvent] = []
    ordered_records[Participant] = []
    ordered_records[Sample] = []
    for n, sample in enumerate(samples):
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

        sample["trial_participant_id"] = sample.pop("participant_id", None)
        cimac_participant_id = cimac_id_to_cimac_participant_id(cimac_id)
        partic = Participant.get_by_id(trial_id, cimac_participant_id)
        if _need_to_insert(
            obj=partic,
            existing=ordered_records[Participant],
            key="cimac_participant_id",
        ):
            new_partic = Participant(
                trial_id=trial_id,
                cimac_participant_id=cimac_participant_id
                ** _get_all_values(target=Participant, old=sample),
            )
            ordered_records[Participant].append(new_partic)

            # if the cohort will be missing, add it
            cohort = Cohort.get_by_id(trial_id, new_partic.cohort_name)
            if _need_to_insert(
                obj=cohort, existing=ordered_records[Cohort], key="cohort_name"
            ):
                new_cohort = Cohort(
                    trial_id=trial_id, cohort_name=new_partic.cohort_name
                )
                ordered_records[Cohort].append(new_cohort)
        else:
            if partic is None:
                partic = [
                    partic.cimac_participant_id == cimac_participant_id
                    for partic in ordered_records[Participant]
                ][0]

            cohort_name = _get_and_check(
                obj=sample,
                key="cohort_name",
                msg=f"Cohort names do not match for participatn {cimac_participant_id} on manifest {manifest_id} for trial {trial_id}\n{partic.cohort_name} [current] != {cohort_name} [new]",
                check=lambda v: v is not None and partic.cohort_name != v,
            )

        new_sample = Sample(
            trial_id=trial_id,
            cimac_participant_id=cimac_id_to_cimac_participant_id(cimac_id),
            **_get_all_values(target=Sample, old=sample),
        )
        ordered_records[Sample].append(new_sample)

        # if the collection event will be missing, add it
        event = CollectionEvent.get_by_id(trial_id, sample.collection_event_name)
        if (
            event is None
            and not any(
                [
                    # differing attribute names means can't use _need_to_insert
                    event.event_name == sample.collection_event_name
                    for event in ordered_records[CollectionEvent]
                ]
            )
            and sample.collection_event_name is not None
        ):
            new_event = CollectionEvent(
                trial_id=trial_id, event_name=sample.collection_event_name
            )
            ordered_records[CollectionEvent].append(new_event)

    return insert_record_batch(ordered_records, session=session)
