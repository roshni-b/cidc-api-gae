from cidc_api.models import ClinicalTrial, Cohort, CollectionEvent, insert_record_batch
from collections import OrderedDict


def set_up_example_trial(cidc_api):
    with cidc_api.app_context():
        to_insert = OrderedDict()
        trial = ClinicalTrial.get_by_id("test_trial")
        if not trial:
            trial = ClinicalTrial(protocol_identifier="test_trial")
            to_insert[ClinicalTrial] = [trial]

        cohort_A = Cohort.get_by_id("test_trial", "Arm_A")
        if not cohort_A:
            cohort_A = Cohort(trial_id=trial.protocol_identifier, cohort_name="Arm_A")
            to_insert[Cohort] = [cohort_A]
        cohort_Z = Cohort.get_by_id("test_trial", "Arm_Z")
        if not cohort_Z:
            if Cohort not in to_insert:
                to_insert[Cohort] = []
            cohort_Z = Cohort(trial_id=trial.protocol_identifier, cohort_name="Arm_Z")
            to_insert[Cohort].append(cohort_Z)

        baseline = CollectionEvent.get_by_id("test_trial", "Baseline")
        if not baseline:
            baseline = CollectionEvent(
                trial_id=trial.protocol_identifier, event_name="Baseline"
            )
            to_insert[CollectionEvent] = [baseline]
        preday1cycle2 = CollectionEvent.get_by_id("test_trial", "Pre_Day_1_Cycle_2")
        if not preday1cycle2:
            if not CollectionEvent in to_insert:
                to_insert[CollectionEvent] = []
            preday1cycle2 = CollectionEvent(
                trial_id=trial.protocol_identifier, event_name="Pre_Day_1_Cycle_2"
            )
            to_insert[CollectionEvent].append(preday1cycle2)

        if len(to_insert):
            errors = insert_record_batch(to_insert)
            assert len(errors) == 0, "\n".join(str(e) for e in errors)

    return trial
