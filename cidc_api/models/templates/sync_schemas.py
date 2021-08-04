from sqlalchemy.orm.session import Session
from typing import Dict, List

from ..models import TrialMetadata, UploadJobs
from .trial_metadata import (
    ClinicalTrial,
    Cohort,
    CollectionEvent,
    Participant,
    Sample,
    Shipment,
)
from .utils import with_default_session


def _make_sample_to_shipment_map(trial_id: str, session: Session) -> Dict[str, str]:
    sample_to_shipment_map = {}

    uploads = (
        session.query(UploadJobs)
        .filter(UploadJobs.trial_id == trial_id, UploadJobs.multifile == False)
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
    errors = []

    trials = session.query(TrialMetadata).all()
    for trial in trials:
        new_trial = ClinicalTrial(
            protocol_identifier=trial.trial_id,
            **{
                c.name: trial.metadata_json[c.name]
                for c in ClinicalTrial.__table__.columns
                if c.name != "protocol_identifier" and c.name in trial.metadata_json
            },
        )
        try:
            new_trial = session.merge(new_trial)
            session.flush()
        except Exception as e:
            errors.append(e)
            continue

        new_events = []
        for event_name in trial.metadata_json["allowed_collection_event_names"]:
            new_events.append(
                CollectionEvent(
                    event_name=event_name, trial_id=new_trial.protocol_identifier
                )
            )
        for n in range(len(new_events)):
            try:
                new_events[n] = session.merge(new_events[n])
            except Exception as e:
                errors.append(e)

        new_cohorts = []
        for cohort_name in trial.metadata_json["allowed_cohort_names"]:
            new_cohorts.append(
                Cohort(cohort_name=cohort_name, trial_id=new_trial.protocol_identifier)
            )
        for n in range(len(new_cohorts)):
            try:
                new_cohorts[n] = session.merge(new_cohorts[n])
            except Exception as e:
                errors.append(e)

        new_shipments = []
        for shipment in trial.metadata_json.get("shipments", []):
            new_shipments.append(
                Shipment(
                    trial_id=new_trial.protocol_identifier,
                    **{
                        c.name: shipment[c.name]
                        for c in Shipment.__table__.columns
                        if c.name in shipment
                    },
                )
            )
        for n in range(len(new_shipments)):
            try:
                new_shipments[n] = session.merge(new_shipments[n])
            except Exception as e:
                errors.append(e)

        session.flush()
        shipment_map = _make_sample_to_shipment_map(trial.trial_id, session)

        participants = trial.metadata_json["participants"]
        for partic in participants:
            # special handling for name change
            partic["trial_participant_id"] = partic.pop("participant_id")

            try:
                new_partic = session.merge(
                    Participant(
                        trial_id=new_trial.protocol_identifier,
                        **{
                            c.name: partic[c.name]
                            for c in Participant.__table__.columns
                            if c.name in partic
                        },
                    )
                )
                session.flush()
            except Exception as e:
                errors.append(e)
                continue

            samples = partic.get("samples", [])
            for sample in samples:
                try:
                    # no need to save these
                    session.merge(
                        Sample(
                            trial_id=new_trial.protocol_identifier,
                            cimac_participant_id=new_partic.cimac_participant_id,
                            shipment_manifest_id=shipment_map[sample["cimac_id"]],
                            **{
                                c.name: sample[c.name]
                                for c in Sample.__table__.columns
                                if c.name in sample
                            },
                        )
                    )
                except Exception as e:
                    errors.append(e)

    # final flush before commit / rollback
    session.flush()

    if dry_run or len(errors):
        session.rollback()
    else:
        session.commit()
    session.close()

    return errors
