from enum import Enum
from .core import MetadataTemplate, WorksheetConfig, Entry
from .model_core import cimac_id_to_cimac_participant_id, identity, insert_record_batch

### Template example ###
from .file_metadata import HandeImage, HandeRecord, HandeUpload

HandeAssay = MetadataTemplate(
    upload_type="hande",
    worksheet_configs=[
        WorksheetConfig(
            "H&E",
            [
                Entry(HandeUpload.trial_id, name="protocol identifier"),
                Entry(HandeUpload.assay_creator, name="assay creator"),
            ],
            {
                "Samples": [
                    Entry(HandeRecord.cimac_id, name="cimac id",),
                    Entry(
                        HandeImage.local_path,
                        name="image file",
                        gcs_uri_format="{protocol identifier}/hande/{cimac id}/image_file.svs",
                    ),
                    Entry(
                        HandeRecord.tumor_tissue_percentage,
                        name="tumor tissue (% total area)",
                    ),
                    Entry(
                        HandeRecord.viable_tumor_percentage,
                        name="viable tumor (% area)",
                    ),
                    Entry(
                        HandeRecord.viable_stroma_percentage,
                        name="viable stroma (% area)",
                    ),
                    Entry(HandeRecord.necrosis_percentage, name="necrosis (% area)",),
                    Entry(HandeRecord.fibrosis_percentage, name="fibrosis (% area)",),
                    Entry(HandeRecord.comment, name="comment",),
                ]
            },
        )
    ],
)
