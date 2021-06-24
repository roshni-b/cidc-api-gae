from sqlalchemy.sql.type_api import to_instance
import pytest

from cidc_api.models import ClinicalTrial, Cohort, CollectionEvent, insert_record_batch


def set_up_example_trial(cidc_api):
    with cidc_api.app_context():
        to_insert = []
        trial = ClinicalTrial.get_by_id("test_trial")
        if not trial:
            trial = ClinicalTrial(protocol_identifier="test_trial")
            to_insert.append(trial)

        cohort_A = Cohort.get_by_id("test_trial", "Arm_A")
        if not cohort_A:
            cohort_A = Cohort(trial_id=trial.protocol_identifier, cohort_name="Arm_A")
            to_insert.append(cohort_A)
        cohort_Z = Cohort.get_by_id("test_trial", "Arm_Z")
        if not cohort_Z:
            cohort_Z = Cohort(trial_id=trial.protocol_identifier, cohort_name="Arm_Z")
            to_insert.append(cohort_Z)

        baseline = CollectionEvent.get_by_id("test_trial", "Baseline")
        if not baseline:
            baseline = CollectionEvent(
                trial_id=trial.protocol_identifier, event_name="Baseline"
            )
            to_insert.append(baseline)
        preday1cycle2 = CollectionEvent.get_by_id("test_trial", "Pre_Day_1_Cycle_2")
        if not preday1cycle2:
            preday1cycle2 = CollectionEvent(
                trial_id=trial.protocol_identifier, event_name="Pre_Day_1_Cycle_2"
            )
            to_insert.append(preday1cycle2)

        if len(to_insert):
            errors = insert_record_batch(to_insert)
            assert len(errors) == 0, "\n".join(str(e) for e in errors)

    return trial
