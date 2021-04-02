import re
import csv
import hashlib
from datetime import datetime, timedelta
from enum import Enum as EnumBaseClass
from functools import wraps
from io import BytesIO
from typing import BinaryIO, Dict, Optional, List, Union, Callable, Tuple

import pandas as pd
from flask import current_app as app
from google.cloud.storage import Blob
from sqlalchemy import (
    Column,
    Boolean,
    DateTime,
    Integer,
    BigInteger,
    String,
    Enum,
    Index,
    func,
    CheckConstraint,
    ForeignKeyConstraint,
    UniqueConstraint,
    tuple_,
    asc,
    desc,
    update,
    case,
    select,
    literal_column,
    not_,
    literal,
)
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import validates
from sqlalchemy.orm.attributes import flag_modified
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.orm.session import Session
from sqlalchemy.orm.query import Query
from sqlalchemy.sql import text
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.engine import ResultProxy

from cidc_schemas import prism, unprism, json_validation

from .files import (
    build_trial_facets,
    build_data_category_facets,
    get_facet_groups_for_paths,
    facet_groups_to_names,
    details_dict,
    FilePurpose,
    FACET_NAME_DELIM,
)

from ..config.db import BaseModel
from ..config.settings import (
    PAGINATION_PAGE_SIZE,
    MAX_PAGINATION_PAGE_SIZE,
    TESTING,
    INACTIVE_USER_DAYS,
    GOOGLE_MAX_DOWNLOAD_PERMISSIONS,
)
from ..shared import emails
from ..shared.gcloud_client import (
    publish_artifact_upload,
    grant_download_access,
    refresh_intake_access,
    revoke_download_access,
)
from ..config.logging import get_logger

logger = get_logger(__name__)


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
            kwargs["session"] = app.extensions["sqlalchemy"].db.session
        return f(*args, **kwargs)

    return wrapped


def make_etag(args: Union[dict, list]):
    """Make an etag by hashing the representation of the provided `args` dict"""
    argbytes = bytes(repr(args), "utf-8")
    return hashlib.md5(argbytes).hexdigest()


class CommonColumns(BaseModel):  # type: ignore
    """Metadata attributes shared by all resources"""

    __abstract__ = True  # Indicate that this isn't a Table schema

    _created = Column(DateTime, default=func.now())
    _updated = Column(DateTime, default=func.now())
    _etag = Column(String(40))
    id = Column(Integer, primary_key=True, autoincrement=True, nullable=False)

    def compute_etag(self) -> str:
        """Calculate the etag for this instance"""
        columns = self.__table__.columns.keys()
        etag_fields = [getattr(self, c) for c in columns if not c.startswith("_")]
        return make_etag(etag_fields)

    @with_default_session
    def insert(self, session: Session, commit: bool = True, compute_etag: bool = True):
        """Add the current instance to the session."""
        # Compute an _etag if none was provided
        self._etag = self._etag or self.compute_etag()

        session.add(self)
        if commit:
            session.commit()

    @with_default_session
    def update(self, session: Session, changes: dict = None, commit: bool = True):
        """
        Update the current instance if it exists in the session.
        `changes` should be a dictionary mapping column names to updated values.
        """
        # Ensure the record exists in the database
        if not self.find_by_id(self.id):
            raise NoResultFound()

        # Update this record's fields if changes were provided
        if changes:
            for column in self.__table__.columns.keys():
                if column in changes:
                    setattr(self, column, changes[column])

        # Set the _updated field to now
        self._updated = datetime.now()

        # Update the instance etag
        self._etag = self.compute_etag()

        session.merge(self)
        if commit:
            session.commit()

    @with_default_session
    def delete(self, session: Session, commit: bool = True):
        """Delete the current instance from the session."""
        session.delete(self)
        if commit:
            session.commit()

    @classmethod
    @with_default_session
    def list(cls, session: Session, **pagination_args):
        """List records in this table, with pagination support."""
        query = session.query(cls)
        query = cls._add_pagination_filters(query, **pagination_args)
        return query.all()

    @classmethod
    def _add_pagination_filters(
        cls,
        query: Query,
        page_num: int = 0,
        page_size: int = PAGINATION_PAGE_SIZE,
        sort_field: Optional[str] = None,
        sort_direction: Optional[str] = None,
        filter_: Callable[[Query], Query] = lambda q: q,
    ) -> Query:
        # Enforce positive page numbers
        page_num = 0 if page_num < 0 else page_num

        # Enforce maximum page size
        page_size = min(page_size, MAX_PAGINATION_PAGE_SIZE)

        # Handle sorting
        if sort_field:
            # Get the attribute from the class, in case this is a hybrid attribute
            sort_attribute = getattr(cls, sort_field)
            field_with_dir = (
                asc(sort_attribute) if sort_direction == "asc" else desc(sort_attribute)
            )
            query = query.order_by(field_with_dir)

        # Apply filter function
        query = filter_(query)

        # Handle pagination
        query = query.offset(page_num * page_size)
        query = query.limit(page_size)

        return query

    @classmethod
    @with_default_session
    def count(cls, session: Session, filter_: Callable[[Query], Query] = lambda q: q):
        """Return the total number of records in this table."""
        filtered_query = filter_(session.query(cls.id))
        return filtered_query.count()

    @classmethod
    @with_default_session
    def count_by(
        cls, expr, session: Session, filter_: Callable[[Query], Query] = lambda q: q
    ) -> Dict[str, int]:
        """
        Return a dictionary mapping results of `expr` to the number of times each result
        occurs in the table related to this model. E.g., for the `UploadJobs` model, 
        `UploadJobs.count_by_column(UploadJobs.upload_type)` would return a dictionary mapping
        upload types to the number of jobs for each type.
        """
        results = filter_(session.query(expr, func.count(cls.id)).group_by(expr)).all()
        return dict(results)

    @classmethod
    @with_default_session
    def find_by_id(cls, id: int, session: Session):
        """Find the record with this id"""
        return session.query(cls).get(id)

    @classmethod
    @with_default_session
    def get_distinct(
        cls,
        column_name: str,
        session: Session,
        filter_: Callable[[Query], Query] = lambda q: q,
    ):
        """Get a list of distinct values for the given column."""
        assert (
            column_name in cls.__table__.columns.keys()
        ), f"{cls.__tablename__} has no column {column_name}"

        base_query = session.query(getattr(cls, column_name))
        filtered_query = filter_(base_query)
        distinct_query = filtered_query.distinct()

        return list(v[0] for v in distinct_query)


class CIDCRole(EnumBaseClass):
    ADMIN = "cidc-admin"
    CIDC_BIOFX_USER = "cidc-biofx-user"
    CIMAC_BIOFX_USER = "cimac-biofx-user"
    CIMAC_USER = "cimac-user"
    DEVELOPER = "developer"
    DEVOPS = "devops"
    NCI_BIOBANK_USER = "nci-biobank-user"
    NETWORK_VIEWER = "network-viewer"


ROLES = [role.value for role in CIDCRole]
ORGS = ["CIDC", "DFCI", "ICAHN", "STANFORD", "ANDERSON"]


class Users(CommonColumns):
    __tablename__ = "users"

    _accessed = Column(DateTime, default=func.now(), nullable=False)
    email = Column(String, unique=True, nullable=False, index=True)
    contact_email = Column(String)
    first_n = Column(String)
    last_n = Column(String)
    organization = Column(Enum(*ORGS, name="orgs"))
    approval_date = Column(DateTime)
    role = Column(Enum(*ROLES, name="role"))
    disabled = Column(Boolean, default=False, server_default="false")

    @validates("approval_date")
    def send_approval_confirmation(self, key, new_approval_date):
        """Send this user an approval email if their account has just been approved"""
        if self.approval_date is None and new_approval_date is not None:
            emails.confirm_account_approval(self, send_email=True)

        return new_approval_date

    def is_admin(self) -> bool:
        """Returns true if this user is a CIDC admin."""
        return self.role == CIDCRole.ADMIN.value

    def is_nci_user(self) -> bool:
        """Returns true if this user is an NCI Biobank user."""
        return self.role == CIDCRole.NCI_BIOBANK_USER.value

    @with_default_session
    def update_accessed(self, session: Session, commit: bool = True):
        """Set this user's last system access to now."""
        today = datetime.now()
        if not self._accessed or (today - self._accessed).days > 1:
            self._accessed = today
            session.merge(self)
            if commit:
                session.commit()

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
            logger.info(f"Creating new user with email {email}")
            user = Users(email=email)
            user.insert(session=session)
        return user

    @staticmethod
    @with_default_session
    def disable_inactive_users(session: Session, commit: bool = True):
        """
        Disable any users who haven't accessed the API in more than `settings.INACTIVE_USER_DAYS`.
        """
        user_inactivity_cutoff = datetime.today() - timedelta(days=INACTIVE_USER_DAYS)
        update_query = (
            update(Users)
            .where(Users._accessed < user_inactivity_cutoff)
            .values(disabled=True)
            .returning(Users.email)
        )
        res = session.execute(update_query)
        if commit:
            session.commit()
        return res

    @staticmethod
    @with_default_session
    def get_data_access_report(io: BinaryIO, session: Session) -> pd.DataFrame:
        """
        Generate an XLSX containing an overview of trial/assay data access permissions
        for every active user in the database. The report will have a sheet per protocol
        identifier, with each sheet containing columns corresponding to a user's email,
        organization, role, and upload type access permissions.

        Save an excel file to the given file handler, and return the pandas dataframe
        used to generate that excel file.
        """
        user_columns = (Users.email, Users.organization, Users.role)

        query = (
            session.query(
                *user_columns,
                Permissions.trial_id,
                func.string_agg(Permissions.upload_type, ","),
            )
            .filter(
                Users.id == Permissions.granted_to_user,
                Users.disabled == False,
                Users.role != None,
                # Exclude admins, since perms in the Permissions table don't impact them.
                # Admin users are handled below.
                Users.role != CIDCRole.ADMIN.value,
            )
            .group_by(Users.id, Permissions.trial_id)
            .union_all(
                # Handle admins separately, since they can view all data for all
                # trials even if they have no permissions assigned to them.
                session.query(
                    *user_columns, TrialMetadata.trial_id, literal("*")
                ).filter(Users.role == CIDCRole.ADMIN.value)
            )
        )

        df = pd.DataFrame(
            query, columns=["email", "organization", "role", "trial_id", "permissions"]
        )

        with pd.ExcelWriter(io) as writer:
            for trial_id, trial_group in df.groupby("trial_id"):
                trial_group.to_excel(writer, sheet_name=trial_id, index=False)

        return df


class IAMException(Exception):
    pass


class Permissions(CommonColumns):
    __tablename__ = "permissions"
    __table_args__ = (
        ForeignKeyConstraint(
            ["granted_by_user"],
            ["users.id"],
            name="ix_permissions_granted_by_user",
            ondelete="CASCADE",
        ),
        ForeignKeyConstraint(
            ["granted_to_user"],
            ["users.id"],
            name="ix_permissions_granted_to_user",
            ondelete="CASCADE",
        ),
        ForeignKeyConstraint(
            ["trial_id"],
            ["trial_metadata.trial_id"],
            name="ix_permissions_trial_id",
            ondelete="CASCADE",
        ),
        UniqueConstraint(
            "granted_to_user", "trial_id", "upload_type", name="unique_perms"
        ),
    )

    # If user who granted this permission is deleted, this permission will be deleted.
    # TODO: is this what we want?
    granted_by_user = Column(Integer)
    granted_to_user = Column(Integer, nullable=False, index=True)
    trial_id = Column(String, nullable=False, index=True)
    upload_type = Column(String, nullable=False)

    @with_default_session
    def insert(self, session: Session, commit: bool = True, compute_etag: bool = True):
        """
        Insert this permission record into the database and add a corresponding IAM policy binding
        on the GCS data bucket.

        NOTE: values provided to the `commit` argument will be ignored. This method always commits.
        """
        grantee = Users.find_by_id(self.granted_to_user)
        if grantee is None:
            raise IntegrityError(
                params=None,
                statement=None,
                orig=f"`granted_to_user` user must exist, but no user found with id {self.granted_to_user}",
            )

        grantor = None
        if self.granted_by_user is not None:
            grantor = Users.find_by_id(self.granted_by_user)
        else:
            raise IntegrityError(
                params=None,
                statement=None,
                orig=f"`granted_by_user` user must be given",
            )
        if grantor is None:
            raise IntegrityError(
                params=None,
                statement=None,
                orig=f"`granted_by_user` user must exist, but no user found with id {self.granted_by_user}",
            )

        is_network_viewer = grantee.role == CIDCRole.NETWORK_VIEWER.value

        # A user can only have 20 granular permissions at a time, due to GCS constraints
        # (with the exception of Network Viewers, who can't download data from GCS and aren't
        # subject to this constraint.)
        if not is_network_viewer and (
            len(Permissions.find_for_user(self.granted_to_user))
            >= GOOGLE_MAX_DOWNLOAD_PERMISSIONS
        ):
            raise IntegrityError(
                params=None,
                statement=None,
                orig=f"{grantee.email} has greater than or equal to the maximum number of allowed granular permissions ({GOOGLE_MAX_DOWNLOAD_PERMISSIONS}). Remove unused permissions to add others.",
            )

        logger.info(
            f"admin-action: {grantor.email} gave {grantee.email} the permission {self.upload_type} on {self.trial_id}"
        )

        # Always commit, because we don't want to grant IAM download unless this insert succeeds.
        super().insert(session=session, commit=True, compute_etag=compute_etag)

        # Don't make any GCS changes if this user doesn't have download access
        if is_network_viewer:
            return

        try:
            # Grant IAM permission in GCS only if db insert worked
            grant_download_access(grantee.email, self.trial_id, self.upload_type)
        except Exception as e:
            # Delete the just-created permissions record
            super().delete()
            raise IAMException("IAM grant failed.") from e

    @with_default_session
    def delete(
        self, deleted_by: Union[Users, int], session: Session, commit: bool = True
    ):
        """
        Delete this permission record from the database and revoke the corresponding IAM policy binding
        on the GCS data bucket.

        NOTE: values provided to the `commit` argument will be ignored. This method always commits.
        """
        grantee = Users.find_by_id(self.granted_to_user)
        if grantee is None:
            raise NoResultFound(f"no user with id {self.granted_to_user}")

        if not isinstance(deleted_by, Users):
            deleted_by_user = Users.find_by_id(deleted_by)
        else:
            deleted_by_user = deleted_by
        if deleted_by_user is None:
            raise NoResultFound(f"no user with id {deleted_by}")

        # Only make GCS IAM changes if this user has download access
        if grantee.role != CIDCRole.NETWORK_VIEWER.value:
            try:
                # Revoke IAM permission in GCS
                revoke_download_access(grantee.email, self.trial_id, self.upload_type)
            except Exception as e:
                raise IAMException(
                    "IAM revoke failed, and permission db record not removed."
                ) from e

        logger.info(
            f"admin-action: {deleted_by_user.email} removed from {grantee.email} the permission {self.upload_type} on {self.trial_id}"
        )
        super().delete(session=session, commit=True)

    @staticmethod
    @with_default_session
    def find_for_user(user_id: int, session: Session) -> List:
        """Find all Permissions granted to the given user."""
        return session.query(Permissions).filter_by(granted_to_user=user_id).all()

    @staticmethod
    @with_default_session
    def find_for_user_trial_type(
        user_id: int, trial_id: str, type_: str, session: Session
    ):
        """Check if a Permissions record exists for the given user, trial, and type."""
        return (
            session.query(Permissions)
            .filter_by(granted_to_user=user_id, trial_id=trial_id, upload_type=type_)
            .first()
        )

    @staticmethod
    @with_default_session
    def grant_iam_permissions(user: Users, session: Session):
        """
        Grant each of the given `user`'s IAM permissions. If the permissions
        have already been granted, calling this will extend their expiry date.
        """
        # Don't make any GCS changes if this user doesn't have download access
        if user.role == CIDCRole.NETWORK_VIEWER.value:
            return

        filter_for_user = lambda q: q.filter(Permissions.granted_to_user == user.id)
        perms = Permissions.list(
            page_size=Permissions.count(session=session, filter_=filter_for_user),
            filter_=filter_for_user,
            session=session,
        )
        for perm in perms:
            # Regrant each permission to reset the TTL for this permission to
            # `settings.INACTIVE_USER_DAYS` from today.
            grant_download_access(user.email, perm.trial_id, perm.upload_type)

        # Regrant all of the user's intake bucket upload permissions, if they have any
        refresh_intake_access(user.email)

    @staticmethod
    @with_default_session
    def grant_all_iam_permissions(session: Session):
        Permissions._change_all_iam_permissions(grant=True, session=session)

    @staticmethod
    @with_default_session
    def revoke_all_iam_permissions(session: Session):
        Permissions._change_all_iam_permissions(grant=False, session=session)

    @staticmethod
    @with_default_session
    def _change_all_iam_permissions(grant: bool, session: Session):
        perms = Permissions.list(page_size=Permissions.count(), session=session)
        for perm in perms:
            user = Users.find_by_id(perm.granted_to_user)
            if user.is_admin() or user.is_nci_user() or user.disabled:
                continue
            if grant:
                grant_download_access(user.email, perm.trial_id, perm.upload_type)
            else:
                revoke_download_access(user.email, perm.trial_id, perm.upload_type)


class ValidationMultiError(Exception):
    """Holds multiple jsonschema.ValidationErrors"""

    pass


trial_metadata_validator: json_validation._Validator = (
    json_validation.load_and_validate_schema(
        "clinical_trial.json", return_validator=True
    )
)

FileBundle = Dict[str, Dict[FilePurpose, List[int]]]


class TrialMetadata(CommonColumns):
    __tablename__ = "trial_metadata"
    # The CIMAC-determined trial id
    trial_id = Column(String, unique=True, nullable=False, index=True)
    metadata_json = Column(JSONB, nullable=False)

    # Create a GIN index on the metadata JSON blobs
    _metadata_idx = Index("metadata_idx", metadata_json, postgresql_using="gin")

    @validates("metadata_json")
    def validate_metadata_json(self, key, metadata_json):
        return self._validate_metadata_json(metadata_json)

    @staticmethod
    def _validate_metadata_json(metadata_json):
        errs = trial_metadata_validator.iter_error_messages(metadata_json)
        messages = list(f"'metadata_json': {err}" for err in errs)
        if messages:
            raise ValidationMultiError(messages)
        return metadata_json

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
        trial._etag = make_etag([trial.trial_id, updated_metadata])

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

        logger.info(f"Creating new trial metadata with id {trial_id}")
        trial = TrialMetadata(trial_id=trial_id, metadata_json=metadata_json)
        trial.insert(session=session, commit=commit)

        return trial

    @staticmethod
    def merge_gcs_artifact(
        metadata: dict, upload_type: str, uuid: str, gcs_object: Blob
    ):
        return prism.merge_artifact(
            ct=metadata,
            assay_type=upload_type,  # assay_type is the old name for upload_type
            artifact_uuid=uuid,
            object_url=gcs_object.name,
            file_size_bytes=gcs_object.size,
            uploaded_timestamp=gcs_object.time_created.isoformat(),
            md5_hash=gcs_object.md5_hash,
            crc32c_hash=gcs_object.crc32c,
        )

    @staticmethod
    def merge_gcs_artifacts(
        metadata: dict, upload_type: str, uuids_and_gcs_objects: List[Tuple[str, Blob]]
    ):
        return prism.merge_artifacts(
            metadata,
            [
                prism.ArtifactInfo(
                    upload_type=upload_type,
                    artifact_uuid=uuid,
                    object_url=gcs_object.name,
                    file_size_bytes=gcs_object.size,
                    uploaded_timestamp=gcs_object.time_created.isoformat(),
                    md5_hash=gcs_object.md5_hash,
                    crc32c_hash=gcs_object.crc32c,
                )
                for uuid, gcs_object in uuids_and_gcs_objects
            ],
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

    file_bundle: Optional[FileBundle]

    @classmethod
    @with_default_session
    def list_with_file_bundles(cls, session: Session, **pagination_args):
        """List `TrialMetadata` records from the database along with their `file_bundle`s."""
        # Build a query that collects trials along with all files for that trial
        file_bundle_query = DownloadableFiles.build_file_bundle_query()
        query = session.query(cls, file_bundle_query.c.file_bundle).join(
            file_bundle_query, TrialMetadata.trial_id == file_bundle_query.c.trial_id
        )

        # Apply pagination options to the query
        query = cls._add_pagination_filters(query, **pagination_args)

        # Run the query
        query_results = query.all()

        # Add file bundles to the `file_bundle` attribute on each trial record
        trials: List[TrialMetadata] = []
        for trial, file_bundle in query_results:
            trial.file_bundle = file_bundle
            trials.append(trial)

        return trials

    @classmethod
    def build_trial_filter(cls, user: Users, trial_ids: List[str] = []):
        filters = []
        if trial_ids:
            filters.append(cls.trial_id.in_(trial_ids))
        if not user.is_admin() and not user.is_nci_user():
            permitted_trials = select([Permissions.trial_id]).where(
                Permissions.granted_to_user == user.id
            )
            filters.append(cls.trial_id.in_(permitted_trials))
        # possible TODO: filter by assays in a trial
        return lambda q: q.filter(*filters)

    @classmethod
    @with_default_session
    def get_metadata_counts(cls, session: Session) -> dict:
        """
        Return a dictionary with the following structure:
        ```
            {
                "num_trials": <count of all trials>,
                "num_participants": <count of all participants across all trials>,
                "num_samples": <count of all samples across all participants across all trials>
            }
        ```
        """
        # Count all trials, participants, and samples in the database
        [(num_trials, num_participants, num_samples)] = session.execute(
            """
            SELECT
                COUNT(DISTINCT trial_id),
                COUNT(participants),
                SUM(jsonb_array_length(participants->'samples'))
            FROM
                trial_metadata,
                LATERAL jsonb_array_elements(metadata_json->'participants') participants;
            """
        )

        return {
            "num_trials": num_trials,
            "num_participants": num_participants,
            "num_samples": num_samples,
        }

    @staticmethod
    @with_default_session
    def get_summaries(session: Session) -> List[dict]:
        """
        Return a list of trial summaries, where each summary has structure like:
        ```python
            {
                "trial_id": ...,
                "file_size_bytes": ..., # total file size for the trial
                "wes": ..., # wes sample count
                "cytof": ..., # cytof sample count
                ... # other assays
            }
        ```
        NOTE: if the metadata model for any existing assays substantially changes,
        or if new assays are introduced that don't follow the typical structure 
        (batches containing sample-level records), then this method will need to
        be updated to accommodate those changes.
        """
        # Compute the total amount of data in bytes stored for each trial
        files_subquery = """
            select
                trial_id,
                'file_size_bytes' as key,
                file_size_bytes as value
            from
                downloadable_files
        """

        # Compute the number of samples associated with each assay type for
        # assays whose metadata follows the typical structure: an array of batches,
        # with each batch containing an array of records, where each record
        # corresponds to a unique sample.
        generic_assay_subquery = """
            select
                trial_id,
                case
                    when key = 'cytof_10021' then 'cytof'
                    when key = 'hande' then 'h&e'
                    else key
                end as key,
                jsonb_array_length(batches->'records') as value
            from
                trial_metadata,
                jsonb_each(metadata_json->'assays') assays,
                jsonb_array_elements(value) batches
            where key not in ('olink', 'nanostring', 'elisa', 'cytof_e4412')
        """

        # Compute the number of samples associated with nanostring uploads.
        # Nanostring metadata has a slightly different structure than typical
        # assays, where each batch has an array of runs, and each run has
        # an array of sample-level entries.
        nanostring_subquery = """
            select
                trial_id,
                'nanostring' as key,
                jsonb_array_length(runs->'samples') as value
            from
                trial_metadata,
                jsonb_array_elements(metadata_json#>'{assays,nanostring}') batches,
                jsonb_array_elements(batches->'runs') runs
        """

        # Compute the number of samples associated with olink uploads.
        # Unlike other assays, olink metadata is an object at the top level
        # rather than an array of batches. This object has a "batches"
        # property that points to an array of batches, and each batch contains
        # an array of records. These records are *not* sample-level; rather,
        # the number of samples corresponding to a given record is stored
        # like: record["files"]["assay_npx"]["number_of_samples"].
        olink_subquery = """
            select
                trial_id,
                'olink' as key,
                (records#>'{files,assay_npx,number_of_samples}')::text::integer as value
            from
                trial_metadata,
                jsonb_array_elements(metadata_json#>'{assays,olink,batches}') batches,
                jsonb_array_elements(batches->'records') records
        """

        # Compute the number of samples associated with elisa uploads.
        # Unlike other assays, elisa metadata is an array of entries, each containing a single data file.
        # The number of samples corresponding to a given entry is stored like:
        # entry["assay_xlsx"]["number_of_samples"].
        elisa_subquery = """
            select
                trial_id,
                'elisa' as key,
                (entry#>'{assay_xlsx,number_of_samples}')::text::integer as value
            from
                trial_metadata,
                jsonb_array_elements(metadata_json#>'{assays,elisa}') entry
        """

        # Compute the number of samples associated with cytof_4412 uploads.
        # cytof_e4412 metadata has a slightly different structure than typical
        # assays, where each batch has an array of participants, and each participant has
        # an array of sample-level entries.
        cytof_e4412_subquery = """
            select
                trial_id,
                'cytof' as key,
                jsonb_array_length(participant->'samples') as value
            from
                trial_metadata,
                jsonb_array_elements(metadata_json#>'{assays,cytof_e4412}') batches,
                jsonb_array_elements(batches->'participants') participant
        """

        # All the subqueries produce the same set of columns, so `UNION`
        # them together into a single query, aggregating results into
        # trial-level JSON dictionaries with the shape described in the docstring.
        combined_query = f"""
            select
                jsonb_object_agg(key, value) || jsonb_object_agg('trial_id', trial_id)
            from (
                select
                    trial_id,
                    key,
                    sum(value) as value
                from (
                    {files_subquery}
                    union
                    {generic_assay_subquery}
                    union
                    {nanostring_subquery}
                    union
                    {olink_subquery}
                    union
                    {elisa_subquery}
                    union all
                    {cytof_e4412_subquery}
                ) q1
                group by trial_id, key
            ) q2
            group by trial_id;
        """

        # Run the query and extract the trial-level summary dictionaries
        summaries = [summary for (summary,) in session.execute(combined_query)]

        # Shortcut to impute 0 values for assays where trials don't yet have data
        summaries = pd.DataFrame(summaries).fillna(0).to_dict("records")

        return summaries


class UploadJobStatus(EnumBaseClass):
    STARTED = "started"
    # Set by CLI based on GCS upload results
    UPLOAD_COMPLETED = "upload-completed"
    UPLOAD_FAILED = "upload-failed"
    # Set by ingest_UploadJobs cloud function based on merge / transfer results
    MERGE_COMPLETED = "merge-completed"
    MERGE_FAILED = "merge-failed"

    @classmethod
    def is_valid_transition(
        cls, current: str, target: str, is_manifest: bool = False
    ) -> bool:
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
            if c == cls.STARTED and t in merge_statuses and not is_manifest:
                return False
        return True


UPLOAD_STATUSES = [s.value for s in UploadJobStatus]


class UploadJobs(CommonColumns):
    __tablename__ = "upload_jobs"
    # An upload job must contain a gcs_file_map is it isn't a manifest upload
    __table_args__ = (
        CheckConstraint(f"multifile = true OR gcs_file_map != null"),
        ForeignKeyConstraint(
            ["uploader_email"],
            ["users.email"],
            name="upload_jobs_uploader_email_fkey",
            onupdate="CASCADE",
        ),
        ForeignKeyConstraint(
            ["trial_id"],
            ["trial_metadata.trial_id"],
            name="assay_uploads_trial_id_fkey",
        ),
    )

    # The current status of the upload job
    _status = Column(
        "status", Enum(*UPLOAD_STATUSES, name="upload_job_status"), nullable=False
    )
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
    # The parsed JSON metadata blob associated with this upload
    metadata_patch = Column(JSONB, nullable=False)
    # The type of upload (pbmc, wes, olink, wes_analysis, ...)
    upload_type = Column(String, nullable=False)
    # Link to the user who created this upload.
    uploader_email = Column(String, nullable=False)
    # The trial that this is an upload for.
    trial_id = Column(String, nullable=False, index=True)

    # Create a GIN index on the GCS object names
    _gcs_objects_idx = Index(
        "upload_jobs_gcs_gcs_file_map_idx", gcs_file_map, postgresql_using="gin"
    )

    @hybrid_property
    def status(self):
        return self._status

    @status.setter
    def status(self, status: str):
        """Set the status if given value is valid."""
        # If old status isn't set on this instance, then this instance hasn't
        # yet been saved to the db, so default to the old status to STARTED.
        old_status = self.status or UploadJobStatus.STARTED.value
        is_manifest = self.upload_type in prism.SUPPORTED_MANIFESTS
        if not UploadJobStatus.is_valid_transition(old_status, status, is_manifest):
            raise ValueError(
                f"Upload job with status {self.status} can't transition to status {status}"
            )
        self._status = status

    def _set_status_no_validation(self, status: str):
        """Set the status without performing validations."""
        assert TESTING, "status_no_validation should only be used in tests"
        self._status = status

    def alert_upload_success(self, trial: TrialMetadata):
        """Send an email notification that an upload has succeeded."""
        # Send admin notification email
        emails.new_upload_alert(self, trial.metadata_json, send_email=True)

    def upload_uris_with_data_uris_with_uuids(self):
        for upload_uri, uuid in (self.gcs_file_map or {}).items():
            # URIs in the upload bucket have a structure like (see ingestion.upload_assay)
            # [trial id]/{prismify_generated_path}/[timestamp].
            # We strip off the /[timestamp] suffix from the upload url,
            # since we don't care when this was uploaded.
            target_url = "/".join(upload_uri.split("/")[:-1])

            yield upload_uri, target_url, uuid

    @staticmethod
    @with_default_session
    def create(
        upload_type: str,
        uploader_email: str,
        gcs_file_map: dict,
        metadata: dict,
        gcs_xlsx_uri: str,
        session: Session,
        commit: bool = True,
        send_email: bool = False,
        status: UploadJobStatus = UploadJobStatus.STARTED.value,
    ):
        """Create a new upload job for the given trial metadata patch."""
        assert prism.PROTOCOL_ID_FIELD_NAME in metadata, "metadata must have a trial ID"

        is_manifest_upload = upload_type in prism.SUPPORTED_MANIFESTS
        assert (
            gcs_file_map is not None or is_manifest_upload
        ), "assay/analysis uploads must have a gcs_file_map"

        trial_id = metadata[prism.PROTOCOL_ID_FIELD_NAME]

        job = UploadJobs(
            multifile=is_manifest_upload,
            trial_id=trial_id,
            upload_type=upload_type,
            gcs_file_map=gcs_file_map,
            metadata_patch=metadata,
            uploader_email=uploader_email,
            gcs_xlsx_uri=gcs_xlsx_uri,
            status=status,
        )
        job.insert(session=session, commit=commit)

        if send_email:
            trial = TrialMetadata.find_by_trial_id(trial_id)
            job.alert_upload_success(trial)

        return job

    @staticmethod
    @with_default_session
    def merge_extra_metadata(job_id: int, files: dict, session: Session):
        """
        Args:
            job_id: the ID of the UploadJob to merge
            files: mapping from uuid of the artifact-to-update to metadata file-to-update-from
            session: the current session; uses default if not passed
        Returns:
            None
        Raises:
            ValueError
                if `job_id` doesn't exist or is already merged
                from prism.merge_artifact_extra_metadata
        """

        job = UploadJobs.find_by_id(job_id, session=session)

        if job is None or job.status == UploadJobStatus.MERGE_COMPLETED:
            raise ValueError(f"Upload job {job_id} doesn't exist or is already merged")

        logger.info(f"About to merge extra md to {job.id}/{job.status}")

        for uuid, file in files.items():
            logger.info(f"About to parse/merge extra md on {uuid}")
            (
                job.metadata_patch,
                updated_artifact,
                _,
            ) = prism.merge_artifact_extra_metadata(
                job.metadata_patch, uuid, job.upload_type, file
            )
            logger.info(f"Updated md for {uuid}: {updated_artifact.keys()}")

        # A workaround fix for JSON field modifications not being tracked
        # by SQLalchemy for some reason. Using MutableDict.as_mutable(JSON)
        # in the model doesn't seem to help.
        flag_modified(job, "metadata_patch")

        logger.info(f"Updated {job.id}/{job.status} patch: {job.metadata_patch}")
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
        if not UploadJobStatus.is_valid_transition(
            self.status, UploadJobStatus.MERGE_COMPLETED.value
        ):
            raise Exception(
                f"Cannot declare ingestion success given current status: {self.status}"
            )
        self.status = UploadJobStatus.MERGE_COMPLETED.value

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
    __table_args__ = (
        ForeignKeyConstraint(
            ["trial_id"],
            ["trial_metadata.trial_id"],
            name="downloadable_files_trial_id_fkey",
        ),
    )

    file_name = Column(String, nullable=False)
    file_size_bytes = Column(BigInteger, nullable=False)
    uploaded_timestamp = Column(DateTime, nullable=False)
    facet_group = Column(String, nullable=False)
    # NOTE: this column actually has type CITEXT.
    data_format = Column(String, nullable=False)
    additional_metadata = Column(JSONB, nullable=False)
    # TODO rename upload_type, because we store manifests in there too.
    # NOTE: this column actually has type CITEXT.
    upload_type = Column(String, nullable=False)
    md5_hash = Column(String, nullable=True)
    crc32c_hash = Column(String, nullable=True)
    trial_id = Column(String, nullable=False)
    object_url = Column(String, nullable=False, index=True, unique=True)
    visible = Column(Boolean, default=True)

    # Would a bioinformatician likely use this file in an analysis?
    analysis_friendly = Column(Boolean, default=False)

    # Visualization data columns (should always be nullable)
    clustergrammer = Column(JSONB, nullable=True)
    ihc_combined_plot = Column(JSONB, nullable=True)

    FILE_EXT_REGEX = r"\.([^./]*(\.gz)?)$"

    @hybrid_property
    def file_ext(self):
        match = re.search(self.FILE_EXT_REGEX, self.object_url)
        return match.group(1) if match else None

    @file_ext.expression
    def file_ext(cls):
        return func.substring(cls.object_url, cls.FILE_EXT_REGEX)

    @hybrid_property
    def data_category(self):
        return facet_groups_to_names.get(self.facet_group)

    @data_category.expression
    def data_category(cls):
        return DATA_CATEGORY_CASE_CLAUSE

    @hybrid_property
    def data_category_prefix(self):
        """
        The overarching data category for a file. E.g., files with `upload_type` of
        "cytof"` and `"cytof_analyis"` should both have a `data_category_prefix` of `"CyTOF"`.
        """
        if self.data_category is None:
            return None
        return self.data_category.split(FACET_NAME_DELIM, 1)[0]

    @data_category_prefix.expression
    def data_category_prefix(cls):
        return func.split_part(DATA_CATEGORY_CASE_CLAUSE, FACET_NAME_DELIM, 1)

    @hybrid_property
    def file_purpose(self):
        return details_dict.get(self.facet_group).file_purpose

    @file_purpose.expression
    def file_purpose(cls):
        return FILE_PURPOSE_CASE_CLAUSE

    @property
    def short_description(self):
        return details_dict.get(self.facet_group).short_description

    @property
    def long_description(self):
        return details_dict.get(self.facet_group).long_description

    @property
    def cimac_id(self):
        """
        Extract the `cimac_id` associated with this file, if any, by searching the file's 
        additional metadata for a field with a key like `<some>.<path>.cimac_id`.

        NOTE: this is not a sqlalchemy hybrid_property, and it can't be used directly in queries.
        """
        for key, value in self.additional_metadata.items():
            if key.endswith("cimac_id"):
                return value
        return None

    @validates("additional_metadata")
    def check_additional_metadata_default(self, key, value):
        return {} if value in ["null", None, {}] else value

    @with_default_session
    def get_related_files(self, session: Session) -> list:
        """
        Return a list of file records related to this file. We could define "related"
        in any number of ways, but currently, a related file:
            * is sample-specific, and relates to the same sample as this file if this file 
              has an associated `cimac_id`.
            * isn't sample-specific, and relates to the same `data_category_prefix`.
        """
        # If this file has an associated sample, get other files associated with that sample.
        # Otherwise, get other non-sample-specific files for this trial and data category.
        if self.cimac_id is not None:
            query = text(
                "SELECT DISTINCT downloadable_files.* "
                "FROM downloadable_files, LATERAL jsonb_each_text(additional_metadata) addm_kv "
                "WHERE addm_kv.value LIKE :cimac_id AND trial_id = :trial_id AND id != :id"
            )
            params = {
                "cimac_id": f"%{self.cimac_id}",
                "trial_id": self.trial_id,
                "id": self.id,
            }
            related_files = result_proxy_to_models(
                session.execute(query, params), DownloadableFiles
            )
        else:
            not_sample_specific = not_(
                literal_column("additional_metadata::text").like('%.cimac_id":%')
            )
            related_files = (
                session.query(DownloadableFiles)
                .filter(
                    DownloadableFiles.trial_id == self.trial_id,
                    DownloadableFiles.data_category_prefix == self.data_category_prefix,
                    DownloadableFiles.id != self.id,
                    not_sample_specific,
                )
                .all()
            )

        return related_files

    @staticmethod
    def build_file_filter(
        trial_ids: List[str] = [], facets: List[List[str]] = [], user: Users = None
    ) -> Callable[[Query], Query]:
        """
        Build a file filter function based on the provided parameters. The resultant
        filter can then be passed as the `filter_` argument of `DownloadableFiles.list`
        or `DownloadableFiles.count`.

        Args:
            trial_ids: if provided, the filter will include only files with these trial IDs.
            upload_types: if provided, the filter will include only files with these upload types.
            analysis_friendly: if True, the filter will include only files that are "analysis-friendly".
            non_admin_user_id: if provided, the filter will include only files that satisfy
                this user's data access permissions.
        Returns:
            A function that adds filters to a query against the DownloadableFiles table.
        """
        file_filters = []
        if trial_ids:
            file_filters.append(DownloadableFiles.trial_id.in_(trial_ids))
        if facets:
            facet_groups = get_facet_groups_for_paths(facets)
            file_filters.append(DownloadableFiles.facet_group.in_(facet_groups))
        # Admins and NCI biobank users can view all files
        if user and not user.is_admin() and not user.is_nci_user():
            permissions = Permissions.find_for_user(user.id)
            perm_set = [(p.trial_id, p.upload_type) for p in permissions]
            file_tuples = tuple_(
                DownloadableFiles.trial_id, DownloadableFiles.upload_type
            )
            file_filters.append(file_tuples.in_(perm_set))

        def filter_files(query: Query) -> Query:
            return query.filter(*file_filters)

        return filter_files

    @staticmethod
    @with_default_session
    def create_from_metadata(
        trial_id: str,
        upload_type: str,
        file_metadata: dict,
        session: Session,
        additional_metadata: Optional[dict] = None,
        commit: bool = True,
        alert_artifact_upload: bool = False,
    ):
        """
        Create a new DownloadableFiles record from artifact metadata.
        """

        # Filter out keys that aren't columns
        supported_columns = DownloadableFiles.__table__.columns.keys()
        filtered_metadata = {
            "trial_id": trial_id,
            "upload_type": upload_type,
            "additional_metadata": additional_metadata,
        }

        for key, value in file_metadata.items():
            if key in supported_columns:
                filtered_metadata[key] = value
        # TODO maybe put non supported stuff from file_metadata to some misc jsonb column?

        etag = make_etag(filtered_metadata.values())

        object_url = filtered_metadata["object_url"]
        df = (
            session.query(DownloadableFiles)
            .filter_by(object_url=object_url)
            .with_for_update()
            .first()
        )
        if df:
            df = session.merge(
                DownloadableFiles(id=df.id, _etag=etag, **filtered_metadata)
            )
        else:
            df = DownloadableFiles(_etag=etag, **filtered_metadata)

        df.insert(session=session, commit=commit)

        if alert_artifact_upload:
            publish_artifact_upload(object_url)

        return df

    @staticmethod
    @with_default_session
    def create_from_blob(
        trial_id: str,
        upload_type: str,
        data_format: str,
        facet_group: str,
        blob: Blob,
        session: Session,
        commit: bool = True,
        alert_artifact_upload: bool = False,
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
        df.upload_type = upload_type
        df.data_format = data_format
        df.facet_group = facet_group
        df.object_url = blob.name
        df.file_name = blob.name
        df.file_size_bytes = blob.size
        df.md5_hash = blob.md5_hash
        df.crc32c_hash = blob.crc32c
        df.uploaded_timestamp = blob.time_created

        df.insert(session=session, commit=commit)

        if alert_artifact_upload:
            publish_artifact_upload(blob.name)

        return df

    @staticmethod
    @with_default_session
    def get_by_object_url(object_url: str, session: Session):
        """
        Look up the downloadable file record associated with
        the given GCS object url.
        """
        return session.query(DownloadableFiles).filter_by(object_url=object_url).one()

    @classmethod
    @with_default_session
    def list_object_urls(
        cls, ids: List[int], session: Session, filter_: Callable[[Query], Query]
    ) -> List[str]:
        """Get all object_urls for a batch of downloadable file record IDs"""
        query = session.query(cls.object_url).filter(cls.id.in_(ids))
        query = filter_(query)
        return [r[0] for r in query.all()]

    @classmethod
    def build_file_bundle_query(cls) -> Query:
        """
        Build a query that selects nested file bundles from the downloadable files table.
        The `file_bundles` query below should produce one bundle per unique `trial_id` that
        appears in the downloadable files table. Each bundle will have shape like:
        ```
          {
              <type 1>: {
                <purpose 1>: [<file id 1>, <file id 2>, ...],
                <purpose 2>: [...]
              },
              <type 2>: {...}
          }
        ```
        where "type" is something like `"Olink"` or `"Participants Info"` and "purpose" is a `FilePurpose` string.
        """
        tid_col, type_col, purp_col, ids_col, purps_col = (
            literal_column("trial_id"),
            literal_column("type"),
            literal_column("purpose"),
            literal_column("ids"),
            literal_column("purposes"),
        )

        id_bundles = (
            select(
                [
                    cls.trial_id,
                    cls.data_category_prefix.label(type_col.key),
                    cls.file_purpose.label(purp_col.key),
                    func.json_agg(cls.id).label(ids_col.key),
                ]
            )
            .group_by(cls.trial_id, cls.data_category_prefix, cls.file_purpose)
            .alias("id_bundles")
        )
        purpose_bundles = (
            select(
                [
                    tid_col,
                    type_col,
                    func.json_object_agg(
                        func.coalesce(purp_col, "miscellaneous"), ids_col
                    ).label(purps_col.key),
                ]
            )
            .select_from(id_bundles)
            .group_by(tid_col, type_col)
            .alias("purpose_bundles")
        )
        file_bundles = (
            select(
                [
                    tid_col.label(tid_col.key),
                    func.json_object_agg(
                        func.coalesce(type_col, "other"), purps_col
                    ).label("file_bundle"),
                ]
            )
            .select_from(purpose_bundles)
            .group_by(tid_col)
            .alias("file_bundles")
        )
        return file_bundles

    @classmethod
    @with_default_session
    def get_total_bytes(cls, session: Session) -> int:
        """Get the total number of bytes of data stored across all files."""
        total_bytes = session.query(func.sum(cls.file_size_bytes)).one()[0]
        return int(total_bytes)

    @classmethod
    @with_default_session
    def get_trial_facets(
        cls, session: Session, filter_: Callable[[Query], Query] = lambda q: q
    ):
        trial_file_counts = cls.count_by(
            cls.trial_id,
            session=session,
            # Apply the provided filter, and also exclude files with null `data_category`s
            filter_=lambda q: filter_(q).filter(cls.data_category != None),
        )
        trial_facets = build_trial_facets(trial_file_counts)
        return trial_facets

    @classmethod
    @with_default_session
    def get_data_category_facets(
        cls, session: Session, filter_: Callable[[Query], Query] = lambda q: q
    ):
        data_category_file_counts = cls.count_by(
            cls.data_category, session=session, filter_=filter_
        )
        data_category_facets = build_data_category_facets(data_category_file_counts)
        return data_category_facets


# Query clause for computing a downloadable file's data category.
# Used above in the DownloadableFiles.data_category computed property.
DATA_CATEGORY_CASE_CLAUSE = case(
    [(DownloadableFiles.facet_group == k, v) for k, v in facet_groups_to_names.items()]
)

# Query clause for computing a downloadable file's file purpose.
# Used above in the DownloadableFiles.file_purpose computed property.
FILE_PURPOSE_CASE_CLAUSE = case(
    [
        (DownloadableFiles.facet_group == facet_group, file_details.file_purpose)
        for facet_group, file_details in details_dict.items()
    ]
)


def result_proxy_to_models(
    result_proxy: ResultProxy, model: BaseModel
) -> List[BaseModel]:
    """Materialize a sqlalchemy `result_proxy` iterable as a list of `model` instances"""
    return [model(**dict(row_proxy.items())) for row_proxy in result_proxy]
