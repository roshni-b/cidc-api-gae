from sqlalchemy import (
    ARRAY,
    Boolean,
    Column,
    Date,
    Enum as SqlEnum,
    ForeignKey,
    Number,
    String,
)
from sqlalchemy.orm import relationship

from .models import CommonColumns

AssaysEnum = SqlEnum(
    "Olink",
    "WES",
    "RNAseq",
    "IHC",
    "CyTOF",
    "H&E",
    "ELISA",
    "mIF",
    "mIHC",
    "TCRseq",
)
ConcentrationUnits = SqlEnum(
    "Nanogram per Microliter",
    "Milligram per Milliliter",
    "Micrograms per Microliter",
    "Cells per Vial",
    "Not Reported",
    "Other",
)
MaterialUnits = SqlEnum(
    "Microliters",
    "Milliliters",
    "Nanogram per Microliter",
    "Milligram per Milliliter",
    "Micrograms per Microliter",
    "Cells per Vial",
    "Slides",
    "Not Reported",
    "Other",
)
Replacement = SqlEnum(
    "Replacement Not Requested",
    "Replacement Requested",
    "Replacement Tested",
    "Not Reported",
    "Other",
)
VolumeUnits = SqlEnum("Microliter", "Milliliter", "Not Reported", "Other")


class ClinicalTrial(CommonColumns):
    protocol_identifier = Column(String, nullable=False, unique=True)
    nct_id = Column(String)
    nci_id = Column(String)
    trial_name = Column(String)
    trial_description = Column(String)
    trial_organization = Column(String)
    grant_or_affiliated_network = Column(String)
    trial_status = Column(String)
    biobank = Column(String)
    lead_cimac_pis = Column(String)
    lead_cimac_contacts = Column(String)
    lead_trial_staff = Column(String)
    justification = Column(String)
    biomarker_plan = Column(String)
    data_sharing_plan = Column(String)

    # expected_assays : List[str]
    # assays : List[Assay]
    # analysis : List[Analysis]
    # clinical_data : ClinicalData
    # schema : artifact_image

    cohort_list = relationship("Cohort", back_populates="trial")
    collection_event_list = relationship("CollectionEvent", back_populates="trial")
    shipments = relationship("Shipment", back_populates="trial")
    participants = relationship("Participant", back_populates="trial")

    @property
    def allowed_cohort_names(self):
        return [c.name for c in self.cohort_list]

    @property
    def allowed_collection_event_names(self):
        return [ce.name for ce in self.collection_event_list]


class Cohort(CommonColumns):
    trial_id = Column(Integer, ForeignKey(ClinicalTrial.id), nullable=False)
    cohort_name = Column(String, nullable=False)

    trial = relationship(ClinicalTrial, back_populates="allowed_cohort_names")


class CollectionEventSpecimenTypes(CommonColumns):
    collection_event_id = Column(Integer, ForeignKey("CollectionEvent.id"), nullable=False)
    specimen_type_id = Column(Integer, ForeignKey("SpecimenTypes.id"), nullable)

    collection_event = relationship("CollectionEvent", back_populates="specimen_types")


class CollectionEvent(CommonColumns):
    trial_id = Column(Integer, ForeignKey(ClinicalTrial.id), nullable=False)
    event_name = Column(String, nullable=False)

    samples = relationship("Sample", back_populates="collection_event")
    specimen_types = relationship(CollectionEventSpecimenTypes, back_populates="collection_event")
    trial = relationship(ClinicalTrial, back_populates="collection_event_list")


class SpecimenTypes(CommonColumns):
    specimen_type = Column(String, nullable=False)
    intended_assays = Column(AssaysEnum)
    parent_type_id = Column(Integer, ForeignKey("SpecimenTypes.id"), nullable=True)

    derivatives = relationship("SpecimenTypes", back_populates="parent_type")
    parent_type = relationship("SpecimenTypes", back_populates="derivatives")


class Participant(CommonColumns):
    trial_id = Column(Integer, ForeignKey(ClinicalTrial.id), nullable=False)
    cimac_participant_id = Column(String, nullable=False)
    cidc_participant_id = Column(String)
    participant_id = Column(String, nullable=False)
    cohort_id = Column(Integer, ForeignKey(Cohort.id))
    gender = Column(SqlEnum("Male", "Female", "Not Specified", "Other"))
    race = Column(
        SqlEnum(
            "American Indian/Alaska Native",
            "Asian",
            "Black/African American",
            "Native Hawaiian/Pacific Islander",
            "White",
            "Not Reported",
            "Unknown",
            "Other",
        )
    )
    ethnicity = Column(
        SqlEnum(
            "Hispanic or Latino",
            "Not Hispanic or Latino",
            "Not reported",
            "Unknown",
            "Other",
        )
    )

    # clinical: dict

    trial = relationship("ClinicalTrial", back_populates="participants")
    samples = relationship("Sample", back_populates="participant")

    @property
    def cohort_name(self):
        return self.cohort.name


class Sample(CommonColumns):
    cimac_id = Column(String, nullable=False)
    cidc_id = Column(String)
    shipping_entry_number = Column(Integer)
    box_number = Column(String)
    surgical_pathology_report_id = Column(String)
    clinical_report_id = Column(String)
    parent_sample_id = Column(String, nullable=False)
    processed_sample_id = Column(String)
    site_description = Column(String)
    topography_code = Column(String)
    topography_description = Column(String)
    histology_behavior = Column(String)
    histology_behavior_description = Column(String)
    collection_event_id = Column(
        Integer, ForeignKey(CollectionEvent.id), nullable=False
    )
    sample_location = Column(String, nullable=False)
    sample_type_id = Column(Integer, ForeignKey(SampleType.id), nullable=False)
    type_of_tumor_sample = Column(
        SqlEnum("Metastatic Tumor", "Primary Tumor", "Not Reported", "Other")
    )
    sample_collection_procedure = Column(
        SqlEnum(
            "Blood Draw",
            "Excision",
            "Core Biopsy",
            "Punch Biopsy",
            "Endoscopic Biopsy",
            "Bone Marrow Core Biopsy",
            "Bone Marrow Aspirate",
            "Lumbar Puncture",
            "Aspirate",
            "Fine-Needle Aspiration",
            "Not Reported",
            "Other",
        )
    )
    core_number = Column(Integer)
    fixation_stabilization_type = Column(
        SqlEnum(
            "Archival FFPE",
            "Fresh Specimen",
            "Frozen Specimen",
            "Formalin-Fixed Paraffin-Embedded",
            "Optimum cutting temperature medium",
            "Thaw-Lyse",
            "Not Reported",
            "Other",
        )
    )
    type_of_primary_container = Column(
        SqlEnum(
            "Sodium heparin",
            "Blood specimen container with EDTA",
            "Potassium EDTA",
            "Streck Blood Collection Tube",
            "Stool collection container with DNA stabilizer",
            "Not Reported",
            "Other",
        )
    )
    sample_volume = Column(Number)
    sample_volume_units = Column(VolumeUnits)
    processed_sample_type_id = Column(Integer, ForeignKey(SampleType.id))
    processed_sample_volume = Column(Number)
    processed_sample_volume_units = Column(VolumeUnits)
    processed_sample_concentration = Column(Number)
    processed_sample_concentration_units = Column(ConcentrationUnits)
    processed_sample_quantity = Column(Number)
    processed_sample_derivative = Column(
        SqlEnum(
            "Tumor DNA",
            "Tumor RNA",
            "Germline DNA",
            "Circulating Tumor-Derived DNA",
            "Not Reported",
            "Other",
        )
    )
    sample_derivative_volume = Column(Number)
    sample_derivative_volume_units = Column(VolumeUnits)
    sample_derivative_concentration = Column(Number)
    sample_derivative_concentration_units = Column(ConcentrationUnits)
    tumor_tissue_total_area_percentage = Column(Number)
    viable_tumor_area_percentage = Column(Number)
    viable_stroma_area_percentage = Column(Number)
    necrosis_area_percentage = Column(Number)
    fibrosis_area_percentage = Column(Number)
    din = Column(Number)
    a260_a280 = Column(Number)
    a260_a230 = Column(Number)
    pbmc_viability = Column(Number)
    pbmc_recovery = Column(Number)
    pbmc_resting_period_used = Column(SqlEnum("Yes", "No", "Not Reported", "Other"))
    material_used = Column(Number)
    material_used_units = Column(MaterialUnits)
    material_remaining = Column(Number)
    material_remaining_units = Column(MaterialUnits)
    material_storage_condition = Column(
        SqlEnum("RT", "4oC", "(-20)oC", "(-80)oC", "LN", "Not Reported", "Other")
    )
    quality_of_sample = Column(SqlEnum("Pass", "Fail", "Not Reported", "Other"))
    sample_replacement = Column(Replacement)
    residual_sample_use = Column(
        SqlEnum(
            "Sample Returned",
            "Sample Sent to Another Lab",
            "Sample received from CIMAC",
            "Not Reported",
            "Other",
        )
    )
    comments = Column(String)
    diagnosis_verification = Column(
        SqlEnum(
            "Local pathology review was not consistent",
            "Local pathology review was consistent with site of tissue procurement diagnostic pathology report",
            "Not Available",
            "Not Reported",
            "Other",
        )
    )
    intended_assay = Column(String)

    aliquots = relationship("Aliquot", back_populates="sample")
    collection_event = relationship("CollectionEvent", back_populates="samples")
    participant = relationship("Participant", back_populates="samples")
    processed_sample_type = relationship(
        "SampleType", primaryjoin="Sample.processed_sample_type_id == SampleType.id"
    )
    sample_type = relationship(
        "SampleType", primaryjoin="Sample.sample_type_id == SampleType.id"
    )

    # if teype_of_sample == "Blood"
    # type_of_primary_container is not None

    @property
    def collection_event_name(self):
        return self.collection_event.name

    @property
    def type_of_sample(self):
        return self.sample_type.name


class Aliquot(CommonColumns):
    sample_id = Column(Integer, ForeignKey(Sample.id), nullable=False)
    slide_number = Column(String, nullable=False)
    quantity = Column(Integer)
    aliquot_replacement = Column(Replacement, nullable=False)
    aliquot_status = Column(
        SqlEnum(
            "Aliquot Returned",
            "Aliquot Exhausted",
            "Remainder used for other Assay",
            "Aliquot Leftover",
            "Other",
        ),
        nullable=False,
    )
    material_extracted = Column(SqlEnum("DNA", "RNA", "cfDNA", "Other"))
    extracted_concentration = Column(String)
    aliquot_amount = Column(
        String
    )  # RECHECK Should this be moved to Sample or renamed?
    lymphocyte_influx = Column(String)

    sample = relationship(Sample, back_populates="aliquots")


class Shipment(CommonColumns):
    trial_id = Column(Integer, ForeignKey(ClinicalTrial.id), nullable=False)

    manifest_id = Column(String, nullable=False)
    assay_priority = Column(
        SqlEnum(
            "1",
            "2",
            "3",
            "4",
            "5",
            "6",
            "7",
            "8",
            "9",
            "10",
            "11",
            "12",
            "13",
            "14",
            "15",
            "Not Reported",
            "Other",
        ),
        nullable=False,
    )
    assay_type = Column(
        AssaysEnum,
        nullable=False,
    )
    courier = Column(
        SqlEnum("FEDEX", "USPS", "UPS", "Inter-Site Delivery"), nullable=False
    )
    tracking_number = Column(String, nullable=False)
    account_number = Column(String, nullable=False)
    shipping_condition = Column(
        SqlEnum(
            "Frozen_Dry_Ice",
            "Frozen_Shipper",
            "Ice_Pack",
            "Ambient",
            "Not Reported",
            "Other",
        ),
        nullable=False,
    )
    date_shipped = Column(Date, nullable=False)
    date_received = Column(Date, nullable=False)
    quality_of_shipment = Column(
        SqlEnum(
            "Specimen shipment received in good condition",
            "Specimen shipment received in poor condition",
            "Not Reported",
            "Other",
        ),
        nullable=False,
    )
    ship_from = Column(String, nullable=False)
    ship_to = Column(String, nullable=False)
    receiving_party = Column(
        SqlEnum(
            "MDA_Wistuba",
            "MDA_Bernatchez",
            "MDA_Al-Atrash",
            "MSSM_Gnjatic",
            "MSSM_Rahman",
            "MSSM_Kim-Schulze",
            "MSSM_Bongers",
            "DFCI_Wu",
            "DFCI_Hodi",
            "DFCI_Severgnini",
            "DFCI_Livak",
            "Broad_Cibulskis",
            "Stanf_Maecker",
            "Stanf_Bendall",
            "NCH",
            "Adaptive",
            "FNLCR_MoCha",
        ),
        nullable=False,
    )

    trial = relationship(ClinicalTrial, back_populates="shipments")
