import os
import hashlib
from enum import Enum as EnumBaseClass
from functools import wraps
from typing import BinaryIO, Optional, List

from flask import current_app as app
from google.cloud.storage import Blob
from sqlalchemy import (
    Column,
    Boolean,
    DateTime,
    ForeignKey,
    Integer,
    BigInteger,
    String,
    VARCHAR,
    Enum,
    Index,
    func,
)
from sqlalchemy.orm import relationship
from sqlalchemy.orm.attributes import flag_modified
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.orm.session import Session
from sqlalchemy.dialects.postgresql import JSONB, ARRAY, BYTEA
from sqlalchemy.ext.declarative import declarative_base, declared_attr
from eve_sqlalchemy.config import DomainConfig, ResourceConfig

from cidc_schemas import prism, unprism

## Constants
ORGS = ["CIDC", "DFCI", "ICAHN", "STANFORD", "ANDERSON"]


class CIDCRole(EnumBaseClass):
    ADMIN = "cidc-admin"
    CIDC_BIOFX_USER = "cidc-biofx-user"
    CIMAC_BIOFX_USER = "cimac-biofx-user"
    CIMAC_USER = "cimac-user"
    DEVELOPER = "developer"
    DEVOPS = "devops"
    NCI_BIOBANK_USER = "nci-biobank-user"


ROLES = [role.value for role in CIDCRole]


class ExtraDataTypes(EnumBaseClass):
    """Classes of downloadable files, in addition to manifest / assay data"""

    PARTICIPANTS_INFO = "participants info"
    SAMPLES_INFO = "samples info"


EXTRA_DATA_TYPES = [typ.value for typ in ExtraDataTypes]

# See: https://github.com/CIMAC-CIDC/cidc-schemas/blob/master/cidc_schemas/schemas/artifacts/artifact_core.json
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


def get_DOMAIN() -> dict:
    """
    Render all cerberus domains for API resources and set up method restrictions
    and role-based access controls.
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

    # Restrict operations on the 'new_users' resource:
    # * A new_user cannot be created with a role or an approval date
    # * A new_user can _only_ be created (not updated)
    del domain["new_users"]["schema"]["role"]
    del domain["new_users"]["schema"]["approval_date"]
    domain["new_users"]["item_methods"] = []
    domain["new_users"]["resource_methods"] = ["POST"]

    # Restrict operations on resources that only admins should be able to access
    for resource in ["users", "trial_metadata"]:
        domain[resource]["allowed_roles"] = [CIDCRole.ADMIN.value]
        domain[resource]["allowed_item_roles"] = [CIDCRole.ADMIN.value]

    # Restrict operations on the 'permissions' resource:
    # * Only admins can write or update 'permissions'
    # * All users can list the 'permissions' resource, but the results will be filtered by
    #   services.permissions.update_permission_filters to only include their permissions
    domain["permissions"]["allowed_write_roles"] = [CIDCRole.ADMIN.value]
    domain["permissions"]["allowed_item_roles"] = [CIDCRole.ADMIN.value]
    domain["permissions"]["item_methods"] = ["GET", "DELETE", "PATCH"]

    # Restrict operations on the 'assay_uploads' resource:
    # * only admins can list 'assay_uploads' (TODO: we may want people to be able to view their own uploads)
    # * only admins, cimac biofx users, and cidc biofx users can GET items or PATCH 'assay_uploads'
    admin_cimac_cidc = [
        CIDCRole.ADMIN.value,
        CIDCRole.CIMAC_BIOFX_USER.value,
        CIDCRole.CIDC_BIOFX_USER.value,
    ]
    domain["assay_uploads"]["allowed_read_roles"] = [CIDCRole.ADMIN.value]
    domain["assay_uploads"]["allowed_item_read_roles"] = admin_cimac_cidc
    domain["assay_uploads"]["allowed_write_roles"] = admin_cimac_cidc
    domain["assay_uploads"]["allowed_item_write_roles"] = admin_cimac_cidc
    domain["assay_uploads"]["resource_methods"] = ["GET"]
    domain["assay_uploads"]["item_methods"] = ["GET", "PATCH"]

    # Restrict operations on the 'manifest_uploads' resource:
    # * only admins can GET 'manifest_uploads'
    # * only admins and NCI users can GET items or PATCH 'manifest_uploads'
    admin_and_nci = [CIDCRole.ADMIN.value, CIDCRole.NCI_BIOBANK_USER.value]
    domain["manifest_uploads"]["allowed_read_roles"] = [CIDCRole.ADMIN.value]
    domain["manifest_uploads"]["allowed_item_read_roles"] = admin_and_nci
    domain["manifest_uploads"]["allowed_write_roles"] = admin_and_nci
    domain["manifest_uploads"]["allowed_item_write_roles"] = admin_and_nci
    domain["manifest_uploads"]["resource_methods"] = ["GET"]
    domain["manifest_uploads"]["item_methods"] = ["GET", "PATCH"]

    # Restrict operations on the 'downloadable_files' resource:
    # * downloadable_files are read-only through the API
    domain["downloadable_files"]["resource_methods"] = ["GET"]
    domain["downloadable_files"]["item_methods"] = ["GET"]

    # Add the download_link field to the 'downloadable_files' resource schema
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

    assay_type = Column(String, nullable=False)

    @staticmethod
    @with_default_session
    def find_for_user(user: Users, session: Session) -> List:
        """Find all Permissions granted to the given user."""
        return session.query(Permissions).filter_by(granted_to_user=user.id).all()

    @staticmethod
    @with_default_session
    def find_for_user_trial_type(
        user: Users, trial_id: str, type_: str, session: Session
    ):
        """Check if a Permissions record exists for the given user, trial, and type."""
        return (
            session.query(Permissions)
            .filter_by(granted_to_user=user.id, trial_id=trial_id, assay_type=type_)
            .first()
        )


class ValidationMultiError(Exception):
    """Holds multiple jsonschema.ValidationErros"""

    pass


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

        trial = TrialMetadata.select_for_update_by_trial_id(trial_id, session=session)

        # Merge assay metadata into the existing clinical trial metadata
        updated_metadata, errs = prism.merge_clinical_trial_metadata(
            json_patch, trial.metadata_json
        )
        if errs:
            raise ValidationMultiError(errs)
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
    def merge_gcs_artifact(
        metadata: dict, assay_type: str, uuid: str, gcs_object: Blob
    ):
        return prism.merge_artifact(
            ct=metadata,
            assay_type=assay_type,
            artifact_uuid=uuid,
            object_url=gcs_object.name,
            file_size_bytes=gcs_object.size,
            uploaded_timestamp=gcs_object.time_created.isoformat(),
            md5_hash=gcs_object.md5_hash,
            crc32c_hash=gcs_object.crc32c,
        )

    @classmethod
    @with_default_session
    def generate_patient_csv(cls, trial_id: str, session: Session) -> str:
        """Get the current patient CSV for this trial."""
        trial = cls.find_by_trial_id(trial_id, session=session)
        if not trial:
            raise NoResultFound(f"No trial found with id {trial_id}")
        return unprism.unprism_participants(trial.metadata_json)

    @classmethod
    @with_default_session
    def generate_sample_csv(cls, trial_id: str, session: Session) -> str:
        """Get the current sample CSV for this trial."""
        trial = cls.find_by_trial_id(trial_id, session=session)
        if not trial:
            raise NoResultFound(f"No trial found with id {trial_id}")
        return unprism.unprism_samples(trial.metadata_json)


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

    def alert_upload_success(self, trial: TrialMetadata):
        """Send an email notification that an upload has succeeded."""
        # (import these here to avoid a circular import error)
        from cidc_api import emails

        # Send admin notification email
        emails.new_upload_alert(self, trial.metadata_json, send_email=True)


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
        trial: TrialMetadata,
        session: Session,
        commit: bool = True,
        send_email: bool = False,
    ):
        """Create a new ManifestUpload for the given trial manifest patch."""
        assert (
            prism.PROTOCOL_ID_FIELD_NAME in metadata
        ), "metadata patch must have a trial ID"

        assert trial.trial_id == metadata[prism.PROTOCOL_ID_FIELD_NAME]

        upload = ManifestUploads(
            trial_id=trial.trial_id,
            manifest_type=manifest_type,
            metadata_patch=metadata,
            uploader_email=uploader_email,
            gcs_xlsx_uri=gcs_xlsx_uri,
            _etag=make_etag(manifest_type, metadata, uploader_email),
        )
        session.add(upload)
        if commit:
            session.commit()

        if send_email:
            upload.alert_upload_success(trial)

        return upload


class AssayUploadStatus(EnumBaseClass):
    STARTED = "started"
    # Set by CLI based on GCS upload results
    UPLOAD_COMPLETED = "upload-completed"
    UPLOAD_FAILED = "upload-failed"
    # Set by ingest_uploads cloud function based on merge / transfer results
    MERGE_COMPLETED = "merge-completed"
    MERGE_FAILED = "merge-failed"

    @classmethod
    def is_valid_transition(cls, current: str, target: str) -> bool:
        """
        Enforce logic about which state transitions are valid. E.g.,
        an upload whose status is "merge-completed" should never be updated
        to "started".
        """
        c = cls(current)
        t = cls(target)
        upload_statuses = [cls.UPLOAD_COMPLETED, cls.UPLOAD_FAILED]
        merge_statuses = [cls.MERGE_COMPLETED, cls.MERGE_FAILED]
        if c != t:
            if t == cls.STARTED:
                return False
            if c in upload_statuses:
                if t not in merge_statuses:
                    return False
            if c in merge_statuses:
                return False
            if c == cls.STARTED and t in merge_statuses:
                return False
        return True


STATUSES = [s.value for s in AssayUploadStatus]


class AssayUploads(CommonColumns, UploadForeignKeys):
    __tablename__ = "assay_uploads"
    # The current status of the upload job
    status = Column(Enum(*STATUSES, name="assay_upload_status"), nullable=False)
    # Text containing feedback on why the upload status is what it is
    status_details = Column(String, nullable=True)
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
        assert prism.PROTOCOL_ID_FIELD_NAME in metadata, "metadata must have a trial ID"
        trial_id = metadata[prism.PROTOCOL_ID_FIELD_NAME]

        job = AssayUploads(
            trial_id=trial_id,
            assay_type=assay_type,
            gcs_file_map=gcs_file_map,
            assay_patch=metadata,
            uploader_email=uploader_email,
            gcs_xlsx_uri=gcs_xlsx_uri,
            status=AssayUploadStatus.STARTED.value,
            _etag=make_etag(
                assay_type,
                gcs_file_map,
                metadata,
                uploader_email,
                AssayUploadStatus.STARTED.value,
            ),
        )
        session.add(job)
        if commit:
            session.commit()

        return job

    @staticmethod
    @with_default_session
    def merge_extra_metadata(job_id, files, session):

        job = AssayUploads.find_by_id(job_id, session=session)

        print(f"About to merge extra md to {job.id}/{job.status}")

        for uuid, file in files.items():
            print(f"About to parse/merge extra md on {uuid}")
            job.assay_patch, updated_artifact, _ = prism.merge_artifact_extra_metadata(
                job.assay_patch, uuid, job.assay_type, file
            )
            print(f"Updated md for {uuid}: {updated_artifact.keys()}")

        # A workaround fix for JSON field modifications not being tracked
        # by SQLalchemy for some reason. Using MutableDict.as_mutable(JSON)
        # in the model doesn't seem to help.
        flag_modified(job, "assay_patch")

        print(f"Updated {job.id}/{job.status} patch: {job.assay_patch}")
        session.commit()

    @classmethod
    @with_default_session
    def find_by_id_and_email(cls, id, email, session):
        upload = super().find_by_id(id, session=session)
        if upload and upload.uploader_email != email:
            return None
        return upload

    @with_default_session
    def ingestion_success(
        self, trial, session: Session, commit: bool = False, send_email: bool = False
    ):
        """Set own status to reflect successful merge and trigger email notifying CIDC admins."""
        # Do status update if the transition is valid
        if not AssayUploadStatus.is_valid_transition(
            self.status, AssayUploadStatus.MERGE_COMPLETED.value
        ):
            raise Exception(
                f"Cannot declare ingestion success given current status: {self.status}"
            )
        self.status = AssayUploadStatus.MERGE_COMPLETED.value

        if commit:
            session.commit()

        if send_email:
            self.alert_upload_success(trial)


class DownloadableFiles(CommonColumns):
    """
    Store required fields from: 
    https://github.com/CIMAC-CIDC/cidc-schemas/blob/master/cidc_schemas/schemas/artifacts/artifact_core.json
    """

    __tablename__ = "downloadable_files"

    file_name = Column(String, nullable=False)
    file_size_bytes = Column(BigInteger, nullable=False)
    uploaded_timestamp = Column(DateTime, nullable=False)
    # NOTE: this column actually has type CITEXT.
    data_format = Column(String, nullable=False)
    additional_metadata = Column(JSONB, nullable=True)
    # TODO rename assay_type, because we store manifests in there too.
    # NOTE: this column actually has type CITEXT.
    assay_type = Column(String, nullable=False)
    md5_hash = Column(String, nullable=True)
    crc32c_hash = Column(String, nullable=True)
    trial_id = Column(String, ForeignKey("trial_metadata.trial_id"), nullable=False)
    trial = relationship(TrialMetadata, foreign_keys=[trial_id])
    object_url = Column(String, nullable=False, index=True, unique=True)
    visible = Column(Boolean, default=True)

    # Visualization data columns (should always be nullable)
    clustergrammer = Column(JSONB, nullable=True)
    ihc_combined_plot = Column(JSONB, nullable=True)

    @staticmethod
    @with_default_session
    def create_from_metadata(
        trial_id: str,
        assay_type: str,
        file_metadata: dict,
        session: Session,
        additional_metadata: Optional[dict] = None,
        commit: bool = True,
    ):
        """
        Create a new DownloadableFiles record from artifact metadata.
        """

        # Filter out keys that aren't columns
        supported_columns = DownloadableFiles.__table__.columns.keys()
        filtered_metadata = {
            "trial_id": trial_id,
            "assay_type": assay_type,
            "additional_metadata": additional_metadata,
        }
        for key, value in file_metadata.items():
            if key in supported_columns:
                filtered_metadata[key] = value
        # TODO maybe put non supported stuff from file_metadata to some misc jsonb column?

        etag = make_etag(*(filtered_metadata.values()))

        df = (
            session.query(DownloadableFiles)
            .filter_by(object_url=filtered_metadata["object_url"])
            .with_for_update()
            .first()
        )
        if df:
            df = session.merge(
                DownloadableFiles(id=df.id, _etag=etag, **filtered_metadata)
            )
        else:
            df = DownloadableFiles(_etag=etag, **filtered_metadata)

        session.add(df)
        if commit:
            session.commit()

        return df

    @staticmethod
    @with_default_session
    def create_from_blob(
        trial_id: str,
        assay_type: str,
        data_format: str,
        blob: Blob,
        session: Session,
        commit: bool = True,
    ):
        """
        Create a new DownloadableFiles record from from a GCS blob,
        or update an existing one, with the same object_url.
        """

        # trying to find existing one
        df = (
            session.query(DownloadableFiles)
            .filter_by(object_url=blob.name)
            .with_for_update()
            .first()
        )
        if not df:
            df = DownloadableFiles()

        df.trial_id = trial_id
        df.assay_type = assay_type
        df.data_format = data_format
        df.object_url = blob.name
        df.file_name = blob.name
        df.file_size_bytes = blob.size
        df.md5_hash = blob.md5_hash
        df.crc32c_hash = blob.crc32c
        df.uploaded_timestamp = blob.time_created

        session.add(df)

        if commit:
            session.commit()

        return df

    @staticmethod
    @with_default_session
    def get_by_object_url(object_url: str, session: Session):
        """
        Look up the downloadable file record associated with 
        the given GCS object url.
        """
        return session.query(DownloadableFiles).filter_by(object_url=object_url).one()
