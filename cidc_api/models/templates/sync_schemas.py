from sqlalchemy.orm.session import Session
from typing import Any, Dict, List, Optional, Tuple

from ..models import TrialMetadata, UploadJobs
from .model_core import MetadataModel
from .trial_metadata import (
    ClinicalTrial,
    Cohort,
    CollectionEvent,
    Participant,
    Sample,
    Shipment,
)
from .utils import with_default_session, _all_bases


def _make_sample_to_shipment_map(trial_id: str, session: Session) -> Dict[str, str]:
    """
    For a given trial, generate a mapping from a cimac_id to the manifest_id it was loaded.
    Uses the UploadJobs where each patch contains only a single shipment and its samples.
    """
    sample_to_shipment_map = {}

    uploads = (
        session.query(UploadJobs)
        .filter(UploadJobs.trial_id == trial_id, UploadJobs.multifile == False)
        .order_by(UploadJobs._created)
        .all()
    )
    for upload in uploads:
        shipments = upload.metadata_patch.get("shipments", [])
        assert (
            len(shipments) == 1
        ), f"Multiple/no shipments in single upload: {upload.id} on trial {upload.trial_id}"
        manifest_id = shipments[0]["manifest_id"]

        for partic in upload.metadata_patch["participants"]:
            for sample in partic["samples"]:
                sample_to_shipment_map[sample["cimac_id"]] = manifest_id

    return sample_to_shipment_map


@with_default_session
def syncall_from_blobs(session: Session, dry_run: bool = False,) -> List[Exception]:
    """
    Sync all trials from the JSON blobs in TrialMetadata to the new relational tables.
    Handles ClinicalTrial, CollectionEvent, Cohort, Shipment, Participant, and Sample.
    Return a list of all errors encountered.
    """
    errors = []

    trials = session.query(TrialMetadata).all()
    for trial in trials:
        new_errors, new_trial = _generate_new_trial(trial, session)
        errors.extend(new_errors)
        if new_trial is None:
            continue

        errors.extend(_sync_collection_events(trial, new_trial, session))
        errors.extend(_sync_cohorts(trial, new_trial, session))
        errors.extend(_sync_shipments(trial, new_trial, session))

        # Since we'll be referencing these for Participant and Sample
        session.flush()

        errors.extend(_sync_participants_and_samples(trial, new_trial, session))

    # final flush before commit / rollback
    session.flush()

    if dry_run or len(errors):
        session.rollback()
    else:
        session.commit()
    session.close()

    return errors


def _get_all_values(
    target: MetadataModel, old: dict, drop: List[str] = []
) -> Dict[str, Any]:
    """Returns all of the values from `old` that are columns of `target` excepting anything keys in `drop`"""
    columns_to_check = [c for c in target.__table__.columns]
    for b in _all_bases(type(target)):
        if hasattr(b, "__table__"):
            columns_to_check.extend(b.__table__.columns)

    return {
        c.name: old[c.name]
        for c in columns_to_check
        if c.name not in drop and c.name in target
    }


def _generate_new_trial(
    trial: TrialMetadata, session: Session
) -> Tuple[List[Exception], Optional[ClinicalTrial]]:
    """
    Given a TrialMetadata, set up a new ClinicalTrial.
    Returns one of:
        [Exception()], None
        [], ClinicalTrial()
    """
    try:
        new_trial = session.merge(
            ClinicalTrial(
                protocol_identifier=trial.trial_id,
                **_get_all_values(
                    target=ClinicalTrial,
                    old=trial.metadata_json,
                    drop=["protocol_identifier"],
                ),
            )
        )
        session.flush()
        return [], new_trial

    except Exception as e:
        return [e], None


def _sync_collection_events(
    trial: TrialMetadata, new_trial: ClinicalTrial, session: Session
) -> List[Exception]:
    """
    Given corresponding TrialMetadata and ClinicalTrial instances, copy over collection events.
    In JSON, stored as list[string] at /allowed_collection_event_names.
    """
    errors = []
    for event_name in trial.metadata_json["allowed_collection_event_names"]:
        try:
            session.merge(
                CollectionEvent(
                    event_name=event_name, trial_id=new_trial.protocol_identifier
                )
            )
        except Exception as e:
            errors.append(e)

    return errors


def _sync_cohorts(
    trial: TrialMetadata, new_trial: ClinicalTrial, session: Session
) -> List[Exception]:
    """
    Given corresponding TrialMetadata and ClinicalTrial instances, copy over cohorts.
    In JSON, stored as list[string] at /allowed_cohort_names.
    """
    errors = []
    for cohort_name in trial.metadata_json["allowed_cohort_names"]:
        try:
            session.merge(
                Cohort(cohort_name=cohort_name, trial_id=new_trial.protocol_identifier)
            )
        except Exception as e:
            errors.append(e)

    return errors


def _sync_shipments(
    trial: TrialMetadata, new_trial: ClinicalTrial, session: Session
) -> List[Exception]:
    """
    Given corresponding TrialMetadata and ClinicalTrial instances, copy over shipments.
    In JSON, stored as list[shipment] at /shipments, where each shipment is dict[<column name> : <value>]
    """
    errors = []
    for shipment in trial.metadata_json.get("shipments", []):
        try:
            session.merge(
                Shipment(
                    trial_id=new_trial.protocol_identifier,
                    **_get_all_values(target=Shipment, old=shipment),
                )
            )
        except Exception as e:
            errors.append(e)

    return errors


def _sync_participants_and_samples(
    trial: TrialMetadata, new_trial: ClinicalTrial, session: Session
) -> List[Exception]:
    """
    Given corresponding TrialMetadata and ClinicalTrial instances, copy over participants and samples.
    In JSON, stored as list[participant] at /participants, where each participant is dict[<column name> : <value>]
    with /participants/*/samples : list[sample] where each sample is dict[<column name> : <value>].

    For Participant, JSON participant_id becomes relational trial_participant_id for clarity.
    """
    errors = []
    shipment_map = _make_sample_to_shipment_map(trial.trial_id, session)

    participants = trial.metadata_json["participants"]
    for partic in participants:
        # special handling for name change
        partic["trial_participant_id"] = partic.pop("participant_id")

        try:
            # need to keep this for samples
            new_partic = session.merge(
                Participant(
                    trial_id=new_trial.protocol_identifier,
                    **_get_all_values(target=Participant, old=partic),
                )
            )
            # need to flush to add the Participant to then reference on the Sample
            session.flush()
        except Exception as e:
            errors.append(e)
            continue

        samples = partic.get("samples", [])
        for sample in samples:
            try:
                session.merge(
                    Sample(
                        trial_id=new_trial.protocol_identifier,
                        cimac_participant_id=new_partic.cimac_participant_id,
                        shipment_manifest_id=shipment_map[sample["cimac_id"]],
                        **_get_all_values(target=Sample, old=sample),
                    )
                )
            except Exception as e:
                errors.append(e)

    return errors
