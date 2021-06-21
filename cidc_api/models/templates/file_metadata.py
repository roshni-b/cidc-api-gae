from sqlalchemy import (
    Boolean,
    CheckConstraint,
    Column,
    Enum,
    ForeignKey,
    ForeignKeyConstraint,
    Index,
    Integer,
    String,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import relationship

from .model_core import MetadataModel
from .trial_metadata import ClinicalTrial
from cidc_api.models import Users


ArtifactCreator = Enum(
    "DFCI", "Mount Sinai", "Stanford", "MD Anderson", name="artifact_creator_enum"
)
ArtifactCategory = Enum(
    "Assay Artifact from CIMAC",
    "Pipeline Artifact",
    "Manifest File",
    name="artifact_category_enum",
)
UploadStatus = Enum(
    "started", "upload-completed", "upload-failed", "merge-completed", "merge-failed"
)


class Upload(MetadataModel):
    __tablename__ = "uploads"
    id = Column(Integer, autoincrement=True, primary_key=True)
    trial_id = Column(
        String,
        ForeignKey(ClinicalTrial.protocol_identifier),
        primary_key=True,  # both True allows for use as multi Foreign Key
    )

    # The current status of the upload job
    status = Column(UploadStatus, nullable=False)
    # A long, random identifier for this upload job
    token = Column(UUID, server_default=text("gen_random_uuid()"), nullable=False)
    # Text containing feedback on why the upload status is what it is
    status_details = Column(String, nullable=True)
    # Whether the upload contains multiple files
    multifile = Column(Boolean, nullable=False)
    # For multifile UploadJobs, object names for the files to be uploaded mapped to upload_placeholder uuids.
    # For single file UploadJobs, this field is null.
    gcs_file_map = Column(JSONB, nullable=True)
    # track the GCS URI of the .xlsx file used for this upload
    gcs_xlsx_uri = Column(String, nullable=True)
    # The type of upload (pbmc, wes, olink, wes_analysis, ...)
    upload_type = Column(String, nullable=False)
    # Which CIMAC site created the data
    assay_creator = Column(ArtifactCreator, nullable=False)
    # Link to the user who created this upload.
    uploader_email = Column(String, ForeignKey(Users.email), nullable=False)

    # Create a GIN index on the GCS object names
    _gcs_objects_idx = Index(
        "upload_jobs_gcs_gcs_file_map_idx", gcs_file_map, postgresql_using="gin"
    )
    CheckConstraint("multifile or (gcs_file_map is not null)")

    __mapper_args__ = {"polymorphic_on": upload_type, "polymorphic_identity": "base"}


class File(MetadataModel):
    __tablename__ = "files"
    object_url = Column(String, primary_key=True)
    upload_id = Column(
        Integer, primary_key=True
    )  # both True allows for use as multi Foreign Key)
    trial_id = Column(String, nullable=False)
    local_path = Column(String)

    upload_placeholder = Column(String)
    artifact_creator = Column(ArtifactCreator)
    uploader = Column(String)
    file_name = Column(String)
    uploaded_timestamp = Column(String)
    file_size_bytes: Column(Integer)
    md5_hash = Column(String)
    crc32_hash = Column(String)
    visible: Column(Boolean)
    artifact_category = Column(ArtifactCategory)
    data_format = Column(String)
    facet_group = Column(String)

    __table_args__ = (
        ForeignKeyConstraint([trial_id, upload_id], [Upload.trial_id, Upload.id],),
    )
    __mapper_args__ = {"polymorphic_on": data_format, "polymorphic_identity": "base"}


# As the subclasses below do NOT have __tablename__ defined
# they will all be included in `files` with any subclass-specific
# columns being NULL for any other object.

# To use a separate table that will be JOINed, define a table name and add
# object_url = Column(String, ForeignKey(<parent>.object_url), primary_key=True)


class FastaFile(File):
    __mapper_args__ = {"polymorphic_identity": "fasta"}


class Fastq_gzFile(File):
    __mapper_args__ = {"polymorphic_identity": "fastq.gz"}


class Vcf_gzFile(File):
    __mapper_args__ = {"polymorphic_identity": "vcf.gz"}


class ImageFile(File):
    __mapper_args__ = {"polymorphic_identity": "image"}


class VcfFile(File):
    __mapper_args__ = {"polymorphic_identity": "vcf"}


class CsvFile(File):
    __mapper_args__ = {"polymorphic_identity": "csv"}


class TsvFile(File):
    __mapper_args__ = {"polymorphic_identity": "tsv"}


class XlsxFile(File):
    __mapper_args__ = {"polymorphic_identity": "xlsx"}


class NpxFile(File):
    __mapper_args__ = {"polymorphic_identity": "npx"}


class ElisaFile(File):
    __mapper_args__ = {"polymorphic_identity": "elisa"}


class BamFile(File):
    __mapper_args__ = {"polymorphic_identity": "bam"}


class Bam_baiFile(File):
    __mapper_args__ = {"polymorphic_identity": "bam.bai"}


class MafFile(File):
    __mapper_args__ = {"polymorphic_identity": "maf"}


class BinaryFile(File):
    __mapper_args__ = {"polymorphic_identity": "binary"}


class TextFile(File):
    __mapper_args__ = {"polymorphic_identity": "text"}


class ZipFile(File):
    __mapper_args__ = {"polymorphic_identity": "zip"}


class FcsFile(File):
    __mapper_args__ = {"polymorphic_identity": "fcs"}


class GzFile(File):
    __mapper_args__ = {"polymorphic_identity": "gz"}


class RccFile(File):
    __mapper_args__ = {"polymorphic_identity": "rcc"}


class JsonFile(File):
    __mapper_args__ = {"polymorphic_identity": "json"}


class YamlFile(File):
    __mapper_args__ = {"polymorphic_identity": "yaml"}
