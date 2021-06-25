import pytest

from cidc_api.models import ClinicalTrial, Cohort, CollectionEvent, insert_record_batch


def set_up_example_trial(cidc_api):
    with cidc_api.app_context():
        trial = ClinicalTrial(protocol_identifier="test_trial")
        cohorts = [
            Cohort(trial_id=trial.protocol_identifier, cohort_name="Arm_A"),
            Cohort(trial_id=trial.protocol_identifier, cohort_name="Arm_Z"),
        ]
        events = [
            CollectionEvent(trial_id=trial.protocol_identifier, event_name="Baseline"),
            CollectionEvent(
                trial_id=trial.protocol_identifier, event_name="Pre_Day_1_Cycle_2"
            ),
        ]
        errors = insert_record_batch([trial, *cohorts, *events])
        assert len(errors) == 0, "\n".join(str(e) for e in errors)

    return trial
