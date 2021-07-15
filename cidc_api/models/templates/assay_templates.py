from .core import MetadataTemplate, WorksheetConfig, Entry
from .model_core import get_property

### Template example ###
from .assay_metadata import HandeImage, HandeRecord, HandeUpload


HandeAssay = MetadataTemplate(
    upload_type="hande",
    purpose="assay",
    worksheet_configs=[
        WorksheetConfig(
            "H&E",
            [
                Entry(HandeUpload.trial_id, name="protocol identifier"),
                Entry(HandeUpload.assay_creator, name="assay creator"),
            ],
            {
                "Samples": [
                    Entry(HandeRecord.cimac_id, name="cimac id"),
                    Entry(
                        HandeImage.local_path,
                        name="image file",
                        gcs_uri_format="{trial_id}/hande/{cimac_id}/image_file.svs",
                        process_as={HandeRecord.image_url: get_property("object_url")},
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
    constants={HandeUpload.upload_type: "hande", HandeUpload.multifile: True,},
)
