import os
import hashlib
from functools import wraps
from typing import BinaryIO, Optional

from flask import current_app as app
from google.cloud.storage import Blob
from sqlalchemy import (
    Column,
    Boolean,
    DateTime,
    ForeignKey,
    Integer,
    String,
    VARCHAR,
    Enum,
    Index,
    func,
)
from sqlalchemy.orm import relationship
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.orm.session import Session
from sqlalchemy.dialects.postgresql import JSONB, ARRAY, BYTEA
from sqlalchemy.ext.declarative import declarative_base, declared_attr
from eve_sqlalchemy.config import DomainConfig, ResourceConfig

from cidc_schemas import prism

## Constants
ORGS = ["CIDC", "DFCI", "ICAHN", "STANFORD", "ANDERSON"]
ROLES = [
    "cidc-admin",
    "cidc-biofx-user",
    "cimac-biofx-user",
    "cimac-user",
    "developer",
    "devops",
    "nci-biobank-user",
]

# See: https://github.com/CIMAC-CIDC/cidc-schemas/blob/master/cidc_schemas/schemas/artifacts/artifact_core.json
ARTIFACT_CATEGORIES = [
    "Assay Artifact from CIMAC",
    "Pipeline Artifact",
    "Manifest File",
    "Other",
]
ASSAY_CATEGORIES = [
    "Whole Exome Sequencing (WES)",
    "RNASeq",
    "Conventional Immunohistochemistry",
    "Multiplex Immunohistochemistry",
    "Multiplex Immunofluorescence",
    "CyTOF",
    "OLink",
    "NanoString",
    "ELISpot",
    "Multiplexed Ion-Beam Imaging (MIBI)",
    "Other",
    "None",
]
FILE_TYPES = [
    "FASTA",
    "FASTQ",
    "TIFF",
    "VCF",
    "TSV",
    "Excel",
    "NPX",
    "BAM",
    "MAF",
    "PNG",
    "JPG",
    "XML",
    "Other",
]

# TODO: prism should own this functionality...
TRIAL_ID_FIELD = "lead_organization_study_id"
## End constants


def get_DOMAIN() -> dict:
    """
    Render all cerberus domains for data model resources 
    (i.e., any model extending `CommonColumns`).
    """
    domain_config = {}
    domain_config["new_users"] = ResourceConfig(Users)
    domain_config["trial_metadata"] = ResourceConfig(TrialMetadata, id_field="trial_id")
    for model in [Users, ManifestUploads, AssayUploads, Permissions, DownloadableFiles]:
        domain_config[model.__tablename__] = ResourceConfig(model)

    # Eve-sqlalchemy needs this to be specified explicitly for foreign key relations
    related_resources = {
        (Permissions, "to_user"): "users",
        (Permissions, "by_user"): "users",
        (Permissions, "trial"): "trial_metadata",
        (AssayUploads, "uploader"): "users",
        (AssayUploads, "trial"): "trial_metadata",
        (ManifestUploads, "uploader"): "users",
        (ManifestUploads, "trial"): "trial_metadata",
        (DownloadableFiles, "trial"): "trial_metadata",
    }

    domain = DomainConfig(domain_config, related_resources).render()

    # Restrict operations on the 'new_users' resource
    del domain["new_users"]["schema"]["role"]
    del domain["new_users"]["schema"]["approval_date"]
    domain["new_users"]["item_methods"] = []
    domain["new_users"]["resource_methods"] = ["POST"]

    # Make downloadable_files read-only
    domain["downloadable_files"]["allowed_methods"] = ["GET"]
    domain["downloadable_files"]["allowed_item_methods"] = ["GET"]

    # Add the download_link field to the downloadable_files schema
    domain["downloadable_files"]["schema"]["download_link"] = {"type": "string"}

    return domain


def make_etag(*args):
    """Make an _etag by stringify, concatenating, and hashing the provided args"""
    argstr = "|".join([str(arg) for arg in args])
    argbytes = bytes(argstr, "utf-8")
    return hashlib.md5(argbytes).hexdigest()


def with_default_session(f):
    """
    For some `f` expecting a database session instance as a keyword argument,
    set the default value of the session keyword argument to the current app's
    database driver's session. We need to do this in a decorator rather than
    inline in the function definition because the current app is only available
    once the app is running and an application context has been pushed.
    """

    @wraps(f)
    def wrapped(*args, **kwargs):
        if "session" not in kwargs:
            kwargs["session"] = app.data.driver.session
        return f(*args, **kwargs)

    return wrapped


BaseModel = declarative_base()


class CommonColumns(BaseModel):
    """Metadata attributes that Eve uses on all resources"""

    __abstract__ = True  # Indicate that this isn't a Table schema

    _created = Column(DateTime, default=func.now())
    _updated = Column(DateTime, default=func.now(), onupdate=func.now())
    _etag = Column(String(40))
    id = Column(Integer, primary_key=True, autoincrement=True, nullable=False)

    @classmethod
    @with_default_session
    def find_by_id(cls, id: int, session: Session):
        """Find the record with this id"""
        return session.query(cls).get(id)


class Users(CommonColumns):
    __tablename__ = "users"

    email = Column(String, unique=True, nullable=False, index=True)
    first_n = Column(String)
    last_n = Column(String)
    organization = Column(Enum(*ORGS, name="orgs"))
    approval_date = Column(DateTime)
    role = Column(Enum(*ROLES, name="role"))
    disabled = Column(Boolean, default=False, server_default="false")

    @staticmethod
    @with_default_session
    def find_by_email(email: str, session: Session) -> Optional:
        """
            Search for a record in the Users table with the given email.
            If found, return the record. If not found, return None.
        """
        user = session.query(Users).filter_by(email=email).first()
        return user

    @staticmethod
    @with_default_session
    def create(profile: dict, session: Session):
        """
            Create a new record for a user if one doesn't exist
            for the given email. Return the user record associated
            with that email.
        """
        email = profile.get("email")
        first_n = profile.get("given_name")
        last_n = profile.get("family_name")

        user = Users.find_by_email(email)
        if not user:
            print(f"Creating new user with email {email}")
            user = Users(email=email)
            session.add(user)
            session.commit()
        return user


class Permissions(CommonColumns):
    __tablename__ = "permissions"

    # If user who granted this permission is deleted, this permission will be deleted.
    # TODO: is this what we want?
    granted_by_user = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"))
    by_user = relationship("Users", foreign_keys=[granted_by_user])
    granted_to_user = Column(
        Integer, ForeignKey("users.id", ondelete="CASCADE"), index=True
    )
    to_user = relationship("Users", foreign_keys=[granted_to_user])

    trial_id = Column(
        String,
        ForeignKey("trial_metadata.trial_id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    trial = relationship("TrialMetadata", foreign_keys=[trial_id])

    assay_type = Column(Enum(*ASSAY_CATEGORIES, name="assays"), nullable=False)
    mode = Column(Enum("read", "write", name="mode"))


class TrialMetadata(CommonColumns):
    # TODO: split up metadata_json into separate `manifest`, `assays`, and `trial_info` fields on this table.

    __tablename__ = "trial_metadata"
    # The CIMAC-determined trial id
    trial_id = Column(String, unique=True, nullable=False, index=True)
    metadata_json = Column(JSONB, nullable=False)

    # Create a GIN index on the metadata JSON blobs
    _metadata_idx = Index("metadata_idx", metadata_json, postgresql_using="gin")

    @staticmethod
    @with_default_session
    def find_by_trial_id(trial_id: str, session: Session):
        """
            Find a trial by its CIMAC id.
        """
        return session.query(TrialMetadata).filter_by(trial_id=trial_id).first()

    @staticmethod
    @with_default_session
    def select_for_update_by_trial_id(trial_id: str, session: Session):
        """
            Find a trial by its CIMAC id.
        """
        try:
            trial = (
                session.query(TrialMetadata)
                .filter_by(trial_id=trial_id)
                .with_for_update()
                .one()
            )
        except NoResultFound as e:
            raise NoResultFound(f"No trial found with id {trial_id}") from e
        return trial

    @staticmethod
    @with_default_session
    def patch_assays(
        trial_id: str, assay_patch: dict, session: Session, commit: bool = False
    ):
        """
            Applies assay updates to the metadata object from the trial with id `trial_id`.

            TODO: apply this update directly to the not-yet-existent TrialMetadata.manifest field
        """
        return TrialMetadata._patch_trial_metadata(
            trial_id, assay_patch, session=session, commit=commit
        )

    @staticmethod
    @with_default_session
    def patch_manifest(
        trial_id: str, manifest_patch: dict, session: Session, commit: bool = False
    ):
        """
            Applies manifest updates to the metadata object from the trial with id `trial_id`.

            TODO: apply this update directly to the not-yet-existent TrialMetadata.assays field
        """
        return TrialMetadata._patch_trial_metadata(
            trial_id, manifest_patch, session=session, commit=commit
        )

    @staticmethod
    @with_default_session
    def _patch_trial_metadata(
        trial_id: str, json_patch: dict, session: Session, commit: bool = False
    ):
        """
            Applies updates to the metadata object from the trial with id `trial_id`
            and commits current session.

            TODO: remove this function and dependency on it, in favor of separate assay
            and manifest patch strategies.
        """

        trial = TrialMetadata.select_for_update_by_trial_id(trial_id)

        # Merge assay metadata into the existing clinical trial metadata
        updated_metadata = prism.merge_clinical_trial_metadata(
            json_patch, trial.metadata_json
        )
        # Save updates to trial record
        trial.metadata_json = updated_metadata
        trial._etag = make_etag(trial.trial_id, updated_metadata)

        session.add(trial)
        if commit:
            session.commit()

        return trial

    @staticmethod
    @with_default_session
    def create(
        trial_id: str, metadata_json: dict, session: Session, commit: bool = True
    ):
        """
            Create a new clinical trial metadata record.
        """

        print(f"Creating new trial metadata with id {trial_id}")
        trial = TrialMetadata(trial_id=trial_id, metadata_json=metadata_json)
        session.add(trial)

        if commit:
            session.commit()

        return trial

    @staticmethod
    def merge_gcs_artifact(metadata, assay_type, uuid, gcs_object):
        return prism.merge_artifact(
            ct=metadata,
            assay_type=assay_type,
            artifact_uuid=uuid,
            object_url=gcs_object.name,
            file_size_bytes=gcs_object.size,
            uploaded_timestamp=gcs_object.time_created.isoformat(),
            md5_hash=gcs_object.md5_hash,
        )


STATUSES = ["started", "completed", "errored"]


class UploadForeignKeys:
    # Link to the user who created this upload.
    @declared_attr
    def uploader_email(cls):
        return Column(String, ForeignKey("users.email", onupdate="CASCADE"))

    @declared_attr
    def uploader(cls):
        return relationship("Users", foreign_keys=[cls.uploader_email])

    # The trial that this is an upload for.
    # This foreign key constraint means that it won't be possible
    # to create an upload for a trial that doesn't exist.
    @declared_attr
    def trial_id(cls):
        return Column(
            String, ForeignKey("trial_metadata.trial_id"), nullable=False, index=True
        )

    @declared_attr
    def trial(cls):
        return relationship("TrialMetadata", foreign_keys=[cls.trial_id])

    # The object URI for the raw excel form associated with this upload
    gcs_xlsx_uri = Column(String, nullable=False)


class ManifestUploads(CommonColumns, UploadForeignKeys):
    __tablename__ = "manifest_uploads"
    # A type of manifest (pbmc, plasma, ...) this upload is related to
    manifest_type = Column(String, nullable=False)
    # The parsed JSON manifest blob for this upload
    metadata_patch = Column(JSONB, nullable=False)
    # tracks the GCS URI of the .xlsx file used for this upload
    gcs_xlsx_uri = Column(String, nullable=False)

    @staticmethod
    @with_default_session
    def create(
        manifest_type: str,
        uploader_email: str,
        metadata: dict,
        gcs_xlsx_uri: str,
        session: Session,
        commit: bool = True,
    ):
        """Create a new ManifestUpload for the given trial manifest patch."""
        assert TRIAL_ID_FIELD in metadata, "metadata patch must have a trial ID"
        trial_id = metadata[TRIAL_ID_FIELD]

        upload = ManifestUploads(
            trial_id=trial_id,
            manifest_type=manifest_type,
            metadata_patch=metadata,
            uploader_email=uploader_email,
            gcs_xlsx_uri=gcs_xlsx_uri,
            _etag=make_etag(manifest_type, metadata, uploader_email),
        )
        session.add(upload)
        if commit:
            session.commit()

        return upload


class AssayUploads(CommonColumns, UploadForeignKeys):
    __tablename__ = "assay_uploads"
    # The current status of the upload job
    status = Column(Enum(*STATUSES, name="job_statuses"), nullable=False)
    # The object names for the files to be uploaded mapped to upload_placeholder uuids
    gcs_file_map = Column(JSONB, nullable=False)
    # track the GCS URI of the .xlsx file used for this upload
    gcs_xlsx_uri = Column(String, nullable=False)
    # The parsed JSON metadata blob associated with this upload
    assay_patch = Column(JSONB, nullable=False)
    # A type of assay (wes, olink, ...) this upload is related to
    assay_type = Column(String, nullable=False)

    # Create a GIN index on the GCS object names
    _gcs_objects_idx = Index(
        "assay_uploads_gcs_gcs_file_map_idx", gcs_file_map, postgresql_using="gin"
    )

    def upload_uris_with_data_uris_with_uuids(self):
        for upload_uri, uuid in self.gcs_file_map.items():
            # URIs in the upload bucket have a structure like (see ingestion.upload_assay)
            # [trial id]/{prismify_generated_path}/[timestamp].
            # We strip off the /[timestamp] suffix from the upload url,
            # since we don't care when this was uploaded.
            target_url = "/".join(upload_uri.split("/")[:-1])

            yield upload_uri, target_url, uuid

    @staticmethod
    @with_default_session
    def create(
        assay_type: str,
        uploader_email: str,
        gcs_file_map: dict,
        metadata: dict,
        gcs_xlsx_uri: str,
        session: Session,
        commit: bool = True,
    ):
        """Create a new upload job for the given trial metadata patch."""
        assert TRIAL_ID_FIELD in metadata, "metadata must have a trial ID"
        trial_id = metadata[TRIAL_ID_FIELD]

        job = AssayUploads(
            trial_id=trial_id,
            assay_type=assay_type,
            gcs_file_map=gcs_file_map,
            assay_patch=metadata,
            uploader_email=uploader_email,
            gcs_xlsx_uri=gcs_xlsx_uri,
            status="started",
            _etag=make_etag(
                assay_type, gcs_file_map, metadata, uploader_email, "started"
            ),
        )
        session.add(job)
        if commit:
            session.commit()

        return job


class DownloadableFiles(CommonColumns):
    """
    Store required fields from: 
    https://github.com/CIMAC-CIDC/cidc-schemas/blob/master/cidc_schemas/schemas/artifacts/artifact_core.json
    """

    __tablename__ = "downloadable_files"

    file_name = Column(String, nullable=False)
    file_size_bytes = Column(Integer, nullable=False)
    uploaded_timestamp = Column(DateTime, nullable=False)
    artifact_category = Column(
        Enum(*ARTIFACT_CATEGORIES, name="artifact_category"), nullable=False
    )
    data_format = Column(String, nullable=False)
    # TODO rename assay_type, because we store manifests in there too.
    assay_type = Column(String, nullable=False)
    md5_hash = Column(String, nullable=False)
    trial_id = Column(String, ForeignKey("trial_metadata.trial_id"), nullable=False)
    trial = relationship(TrialMetadata, foreign_keys=[trial_id])
    object_url = Column(String, nullable=False)
    visible = Column(Boolean, default=True)

    @staticmethod
    @with_default_session
    def create_from_metadata(
        trial_id: str,
        assay_type: str,
        file_metadata: dict,
        session: Session,
        commit: bool = True,
    ):
        """
        Create a new DownloadableFiles record from a GCS blob.
        """
        etag = make_etag(*(file_metadata.values()))

        # Filter out keys that aren't columns
        supported_columns = DownloadableFiles.__table__.columns.keys()
        filtered_metadata = {"trial_id": trial_id, "assay_type": assay_type}
        for key, value in file_metadata.items():
            if key in supported_columns:
                filtered_metadata[key] = value
        # TODO maybe put non supported stuff from file_metadata to some misc jsonb column?

        new_file = DownloadableFiles(_etag=etag, **filtered_metadata)
        session.add(new_file)
        if commit:
            session.commit()

        return new_file
