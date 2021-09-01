__all__ = [
    # "ClinicalDataTemplate",
    "PbmcManifest",
    "TissueSlideManifest",
]

from sqlalchemy import Column
from typing import Any, Dict, List

from .core import Entry, MetadataTemplate, WorksheetConfig
from .model_core import cimac_id_to_cimac_participant_id, identity

from .trial_metadata import Participant, Sample, Shipment

# from .extra_metadata import ClinicalData


# ClinicalDataTemplate = MetadataTemplate(
#     upload_type="clinical_data",
#     purpose="manifest",
#     worksheet_configs=[
#         WorksheetConfig(
#             "Tier 1",
#             [Entry(ClinicalData.trial_id, name="protocol identifier",),],
#             {
#                 "Demographics": [
#                     Entry(
#                         Participant.cimac_participant_id,
#                         process_as={ClinicalData.cimac_participant_id: identity},
#                     ),
#                     Entry(ClinicalData.race),
#                     Entry(ClinicalData.gender, name="sex",),
#                     Entry(ClinicalData.ethnicity),
#                     Entry(ClinicalData.age),
#                 ],
#                 "History": [
#                     Entry(ClinicalData.prior_surgery),
#                     Entry(ClinicalData.prior_radiation_therapy),
#                     Entry(ClinicalData.prior_immunotherapy),
#                     Entry(ClinicalData.number_prior_systemic_treatments),
#                     Entry(ClinicalData.prior_therapy_type),
#                 ],
#                 "Disease & Baseline": [
#                     Entry(
#                         ClinicalData.mod_ann_arbor_stage,
#                         name="Modified Ann Arbor Stage",
#                     ),
#                     Entry(ClinicalData.ecog_ps),
#                     Entry(ClinicalData.years_from_initial_diagnosis),
#                     Entry(ClinicalData.type_of_most_recent_treatment),
#                     Entry(ClinicalData.response_to_most_recent_treatment),
#                     Entry(ClinicalData.duration_of_remission),
#                     Entry(ClinicalData.years_from_recent_treatment),
#                     Entry(ClinicalData.disease_stage),
#                     Entry(ClinicalData.disease_grade),
#                 ],
#             },
#         ),
#     ],
# )

# To share the base WorksheetConfig's between all of the templates
# we'll use variables to represent where we can add custom elements
# into the otherwise static configs and then return the object
# # not using a subclass so that they are still MetadataTemplate's
# PREFERED: changes may need to be made here to allow for new custom elements
# ALERNATE: can add as post-hoc modifications to individual manifest templates
def _BaseManifestTemplate(
    upload_type: str,
    filled_by_biorepository: List[Entry],
    filled_by_cimac_lab: List[Entry],
    constants: Dict[Column, Any] = {},
):
    return MetadataTemplate(
        upload_type=upload_type,
        purpose="manifest",
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
                    Entry(
                        Shipment.assay_type,
                        process_as={Sample.intended_assay: identity},
                    ),
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
                    "Filled by Biorepository": filled_by_biorepository,
                    "Filled by CIMAC Lab": filled_by_cimac_lab,
                },
            ),
        ],
        constants=constants,
    )


PbmcManifest = _BaseManifestTemplate(
    upload_type="pbmc",
    filled_by_biorepository=[
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
    filled_by_cimac_lab=[
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
)

TissueSlideManifest = _BaseManifestTemplate(
    upload_type="tissue_slide",
    filled_by_biorepository=[
        Entry(Sample.box_number),
        Entry(Sample.sample_location),
        Entry(Sample.type_of_sample),
        Entry(Sample.type_of_tumor_sample),
        Entry(Sample.sample_collection_procedure),
        Entry(Sample.core_number),
        Entry(Sample.fixation_stabilization_type),
        Entry(Sample.processed_sample_type),
        Entry(Sample.processed_sample_quantity),
    ],
    filled_by_cimac_lab=[
        Entry(Sample.material_used),
        Entry(Sample.material_used_units),
        Entry(Sample.material_remaining),
        Entry(Sample.material_remaining_units),
        Entry(Sample.material_storage_condition),
        Entry(Sample.quality_of_sample),
        Entry(Sample.sample_replacement),
        Entry(Sample.residual_sample_use),
        Entry(Sample.comments),
    ],
)
