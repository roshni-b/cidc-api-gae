import os

os.environ["TZ"] = "UTC"
from collections import OrderedDict
from datetime import datetime

from cidc_api.models import (
    ClinicalTrial,
    Cohort,
    CollectionEvent,
    insert_record_batch,
    Participant,
    Sample,
    Shipment,
    Users,
)


def set_up_example_trial(clean_db, cidc_api, insert: bool = True):
    with cidc_api.app_context():
        to_insert = OrderedDict()
        trial = ClinicalTrial(protocol_identifier="test_trial")
        to_insert[ClinicalTrial] = [trial]

        cohort_A = Cohort(trial_id=trial.protocol_identifier, cohort_name="Arm_A")
        cohort_Z = Cohort(trial_id=trial.protocol_identifier, cohort_name="Arm_Z")
        to_insert[Cohort] = [cohort_A, cohort_Z]

        baseline = CollectionEvent(
            trial_id=trial.protocol_identifier, event_name="Baseline"
        )
        preday1cycle2 = CollectionEvent(
            trial_id=trial.protocol_identifier, event_name="Pre_Day_1_Cycle_2"
        )
        to_insert[CollectionEvent] = [baseline, preday1cycle2]

        errors = insert_record_batch(to_insert, session=clean_db)
        assert len(errors) == 0, "\n".join(str(e) for e in errors)

    return to_insert


def setup_example(clean_db, cidc_api):
    with cidc_api.app_context():
        to_insert = OrderedDict()
        trial = ClinicalTrial(protocol_identifier="test_trial")
        to_insert[ClinicalTrial] = [trial]

        cohort_A = Cohort(trial_id="test_trial", cohort_name="Arm_A")
        cohort_Z = Cohort(trial_id="test_trial", cohort_name="Arm_Z")
        cohorts = [cohort_A, cohort_Z]
        to_insert[Cohort] = cohorts

        baseline = CollectionEvent(trial_id="test_trial", event_name="Baseline")
        preday1cycle2 = CollectionEvent(
            trial_id="test_trial", event_name="Pre_Day_1_Cycle_2"
        )
        events = [baseline, preday1cycle2]
        to_insert[CollectionEvent] = events

        user = Users(email="user@email.com")
        to_insert[Users] = [user]

        shipment = Shipment(
            trial_id=trial.protocol_identifier,
            manifest_id="shipment",
            assay_priority="1",
            assay_type="H&E",
            courier="FEDEX",
            tracking_number="foo",
            account_number="foo",
            shipping_condition="Not Reported",
            date_shipped=datetime.strptime("20200101", "%Y%m%d").date(),
            date_received=datetime.strptime("20200101", "%Y%m%d").date(),
            quality_of_shipment="Not Reported",
            ship_from="foo",
            ship_to="foo",
            receiving_party="MDA_Wistuba",
        )
        to_insert[Shipment] = [shipment]

        participant = Participant(
            trial_id=trial.protocol_identifier,
            cimac_participant_id="CTTTPP1",
            trial_participant_id="participant",
        )
        to_insert[Participant] = [participant]

        samples = [
            Sample(
                trial_id=trial.protocol_identifier,
                cimac_id=f"{participant.cimac_participant_id}11.00",
                cimac_participant_id=participant.cimac_participant_id,
                collection_event_name=events[0].event_name,
                manifest_id=shipment.manifest_id,
                parent_sample_id="foo",
                sample_location="foo",
                type_of_sample="Not Reported",
            ),
            Sample(
                trial_id=trial.protocol_identifier,
                cimac_id=f"{participant.cimac_participant_id}21.00",
                cimac_participant_id=participant.cimac_participant_id,
                collection_event_name=events[1].event_name,
                manifest_id=shipment.manifest_id,
                parent_sample_id="foo",
                sample_location="foo",
                type_of_sample="Not Reported",
            ),
        ]
        to_insert[Sample] = samples

        errors = insert_record_batch(to_insert, session=clean_db)
        assert len(errors) == 0, "\n".join(str(e) for e in errors)

        to_insert.move_to_end(Cohort, last=False)
        to_insert.move_to_end(ClinicalTrial, last=False)

    return to_insert
