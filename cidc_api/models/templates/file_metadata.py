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

from .model_core import MetadataModel
from .trial_metadata import ClinicalTrial
from ..models import Users


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
    "started",
    "upload-completed",
    "upload-failed",
    "merge-completed",
    "merge-failed",
    name="upload_status_enum",
)


class Upload(MetadataModel):
    __tablename__ = "uploads"
    id = Column(
        Integer,
        autoincrement=True,
        primary_key=True,
        doc="A unique ID to identify this upload.",
    )
    trial_id = Column(
        String,
        ForeignKey(ClinicalTrial.protocol_identifier),
        primary_key=True,  # both True allows for use as multi Foreign Key
    )

    status = Column(
        UploadStatus,
        nullable=False,
        server_default=UploadStatus.enums[0],  # 'started'
        doc="The current status of the upload.",
    )
    token = Column(
        UUID,
        server_default=text("gen_random_uuid()"),
        nullable=False,
        doc="A long, random identifier for this upload.",
    )
    status_details = Column(
        String,
        nullable=True,
        doc="Text containing feedback on why the upload status is what it is.",
    )
    multifile = Column(
        Boolean, nullable=False, doc="Whether the upload contains multiple files."
    )
    gcs_file_map = Column(
        JSONB,
        nullable=True,
        doc="If multifile, object names for the files to be uploaded mapped to upload_placeholder uuids; else null.",
    )
    gcs_xlsx_uri = Column(
        String,
        nullable=True,
        doc="Track the GCS URI of the .xlsx file used for this upload.",
    )
    upload_type = Column(
        String,
        nullable=False,
        doc="The type of upload (pbmc, wes, olink, wes_analysis, ...)",
    )
    assay_creator = Column(
        ArtifactCreator, nullable=False, doc="Which CIMAC site created the data"
    )
    uploader_email = Column(
        String,
        ForeignKey(Users.email),
        nullable=False,
        doc="Link to the user who created this upload.",
    )

    # Create a GIN index on the GCS object names
    _gcs_objects_idx = Index(
        "upload_gcs_file_map_idx", gcs_file_map, postgresql_using="gin"
    )
    CheckConstraint("multifile or (gcs_file_map is not null)")

    __mapper_args__ = {"polymorphic_on": upload_type, "polymorphic_identity": "base"}

    def __init__(self, **kwargs):
        from ...shared.auth import get_current_user

        kwargs["uploader_email"] = get_current_user().email

        super().__init__(**kwargs)


class File(MetadataModel):
    __tablename__ = "files"
    object_url = Column(String, primary_key=True)
    upload_id = Column(Integer, primary_key=True)
    trial_id = Column(String, primary_key=False)
    local_path = Column(String, unique=True, doc="Path to a file on a user's computer.")

    upload_placeholder = Column(
        String, doc="A placeholder for when artifact file is being uploaded."
    )
    artifact_creator = Column(
        ArtifactCreator, doc="The name of the center that created this artifact."
    )
    uploader = Column(String, doc="The name of the person uploading the artifact.")
    file_name = Column(
        String,
        doc="The name of the file with extension. Generated from 'gcs_uri_format' fields in templates.",
    )
    uploaded_timestamp = Column(
        String, doc="Timestamp of when artifact was loaded into the system."
    )
    file_size_bytes: Column(Integer, doc="File size in bytes.")
    md5_hash = Column(
        String, doc="MD5 Hash of artifact. Not available for composite GCS objects."
    )
    crc32_hash = Column(String, doc="CRC32c Hash of artifact.")
    visible: Column(
        Boolean,
        doc="Indicates if the artifact is visible. If set to false, the artifact is effectively deleted.",
    )
    artifact_category = Column(ArtifactCategory, doc="Artifact category.")
    data_format = Column(String, doc="Data Format.")
    facet_group = Column(String, doc="The internal data category for this artifact")

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
