from .core import MetadataTemplate, WorksheetConfig, Entry
from .model_core import cimac_id_to_cimac_participant_id, identity, insert_record_batch

### Template example ###
from .trial_metadata import Participant, Sample, Shipment


PBMCTemplate = MetadataTemplate(
    upload_type="pbmc",
    worksheet_configs=[
        WorksheetConfig(
            "Shipment",
            [
                Entry(
                    Shipment.trial_id,
                    name="protocol identifier",
                    process_as={
                        Participant.trial_id: identity,
                        Sample.trial_id: identity,
                    },
                ),
                Entry(
                    Shipment.manifest_id,
                    process_as={Sample.shipment_manifest_id: identity},
                ),
                Entry(Shipment.assay_priority),
                Entry(Shipment.assay_type),
                Entry(Shipment.receiving_party),
                Entry(Shipment.courier),
                Entry(Shipment.tracking_number),
                Entry(Shipment.account_number),
                Entry(Shipment.shipping_condition),
                Entry(Shipment.date_shipped),
                Entry(Shipment.date_received),
                Entry(Shipment.quality_of_shipment),
                Entry(Shipment.ship_from),
                Entry(Shipment.ship_to),
            ],
            {},
        ),
        WorksheetConfig(
            "Essential Patient Data",
            [],
            {
                "Path Concordance Verification": [
                    Entry(Participant.trial_participant_id),
                    Entry(
                        Sample.cimac_id,
                        process_as={
                            Participant.cimac_participant_id: cimac_id_to_cimac_participant_id,
                            Sample.cimac_participant_id: cimac_id_to_cimac_participant_id,
                        },
                    ),
                    Entry(Sample.surgical_pathology_report_id),
                    Entry(Sample.clinical_report_id),
                    Entry(Sample.collection_event_name),
                    Entry(Sample.diagnosis_verification),
                ],
                "ICD-0-3 Code/Description": [
                    Entry(Sample.site_description),
                    Entry(Sample.topography_code),
                    Entry(Sample.topography_description),
                    Entry(Sample.histology_behavior),
                    Entry(Sample.histology_behavior_description),
                ],
                "Demographics": [
                    Entry(Participant.gender, name="Sex"),
                    Entry(Participant.race),
                    Entry(Participant.ethnicity),
                ],
            },
        ),
        WorksheetConfig(
            "Samples",
            [],
            {
                "IDs": [
                    Entry(Sample.shipping_entry_number),
                    Entry(Sample.collection_event_name),
                    Entry(Participant.cohort_name),
                    Entry(Participant.trial_participant_id),
                    Entry(Sample.parent_sample_id),
                    Entry(Sample.processed_sample_id),
                    Entry(
                        Sample.cimac_id,
                        process_as={
                            Participant.cimac_participant_id: cimac_id_to_cimac_participant_id
                        },
                    ),
                ],
                "Filled by Biorepository": [
                    Entry(Sample.box_number),
                    Entry(Sample.sample_location),
                    Entry(Sample.type_of_sample),
                    Entry(Sample.sample_collection_procedure),
                    Entry(Sample.type_of_primary_container),
                    Entry(Sample.processed_sample_type),
                    Entry(Sample.processed_sample_volume),
                    Entry(Sample.processed_sample_volume_units),
                    Entry(Sample.processed_sample_concentration),
                    Entry(Sample.processed_sample_concentration_units),
                ],
                "Filled by CIMAC Lab": [
                    Entry(Sample.pbmc_viability),
                    Entry(Sample.pbmc_recovery),
                    Entry(Sample.pbmc_resting_period_used),
                    Entry(Sample.material_used),
                    Entry(Sample.material_remaining),
                    Entry(Sample.material_storage_condition),
                    Entry(Sample.quality_of_sample),
                    Entry(Sample.sample_replacement),
                    Entry(Sample.residual_sample_use),
                    Entry(Sample.comments),
                ],
            },
        ),
    ],
)


if __name__ == "__main__":
    from cidc_api.app import app

    with app.app_context():
        records = PBMCTemplate.read("pbmc_test.xlsx")
        errors = insert_record_batch(records)
        for e in errors:
            print(e.orig)
