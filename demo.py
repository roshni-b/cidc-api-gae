"""Demo of new templates, to be stepped through in the flask shell."""

from cidc_api.models.templates import *

# Get the current session
session = current_app.extensions["sqlalchemy"].db.session

# Make sure db is empty for sake of demo
session.query(Sample).delete()
session.query(Participant).delete()
session.query(Shipment).delete()
session.query(Cohort).delete()
session.query(CollectionEvent).delete()
session.query(ClinicalTrial).delete()
session.commit()

### BEGIN DEMO ###
# Extract a list of database records from the spreadsheet in safe insertion order
records = PBMCTemplate.read("pbmc_test.xlsx")

# Validate the set of records with a "dry run" insert
errors = insert_record_batch(records, dry_run=True)

# We expect errors - we're missing the target trial and the
# relevant cohort and collection event names
[str(e.orig) for e in errors]

# Add the target trial
session.add(ClinicalTrial(protocol_identifier="test_prism_trial_id_PBMC"))
session.add(Cohort(trial_id="test_prism_trial_id_PBMC", cohort_name="Arm_A"))
session.add(Cohort(trial_id="test_prism_trial_id_PBMC", cohort_name="Arm_Z"))
session.commit()

# Try validation again
[str(e.orig) for e in insert_record_batch(records, dry_run=True)]

# Add the missing collection events
session.add(
    CollectionEvent(trial_id="test_prism_trial_id_PBMC", event_name="Pre_Day_1_Cycle_2")
)
session.add(CollectionEvent(trial_id="test_prism_trial_id_PBMC", event_name="Baseline"))
session.commit()

# Try validation again
[str(e.orig) for e in insert_record_batch(records, dry_run=True)]

# Show that we have no shipments, participants, or samples
assert session.query(Shipment).count() == 0
assert session.query(Sample).count() == 0
assert session.query(Participant).count() == 0

# Now do a non-dry run insert
insert_record_batch(records)

# Show that we ingested participants
[participant.cimac_participant_id for participant in session.query(Participant).all()]

# Show that we ingested samples
[sample.cimac_id for sample in session.query(Sample).all()]

# Show that participants and samples are linked
participant = session.query(Participant).first()
participant.samples
