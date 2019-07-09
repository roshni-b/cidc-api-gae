from flask import current_app as app
from sqlalchemy import Column, DateTime, Integer, String, Enum, Index, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.declarative import declarative_base

BaseModel = declarative_base()


class CommonColumns(BaseModel):
    """Metadata attributes that Eve uses on all resources"""

    __abstract__ = True  # Indicate that this isn't a Table schema

    _created = Column(DateTime, default=func.now())
    _updated = Column(DateTime, default=func.now(), onupdate=func.now())
    _etag = Column(String(40))


ORGS = ["CIDC", "DFCI", "ICAHN", "STANFORD", "ANDERSON"]


class Users(CommonColumns):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True, autoincrement=True, nullable=False)
    email = Column(String, unique=True, nullable=False, index=True)
    first_n = Column(String)
    last_n = Column(String)
    organization = Column(Enum(*ORGS, name="orgs"))

    @staticmethod
    def create(email: str):
        """
            Create a new record for a user if one doesn't exist
            for the given email.
        """
        session = app.data.driver.session
        if not session.query(Users).filter_by(email=email).first():
            app.logger.info(f"Creating new user with email {email}")
            session.add(Users(email=email))
            session.commit()


class TrialMetadata(CommonColumns):
    __tablename__ = "trial_metadata"
    id = Column(Integer, primary_key=True, autoincrement=True, nullable=False)
    # The CIMAC-determined trial id
    trial_id = Column(String, unique=True, nullable=False, index=True)
    metadata_json = Column(JSONB, nullable=False)

    # Create a GIN index on the metadata JSON blobs
    _metadata_idx = Index("metadata_idx", metadata_json, postgresql_using="gin")

    @staticmethod
    def patch_trial_metadata(trial_id: str, metadata: dict):
        """
            Applies updates to an existing trial metadata record,
            or create a new one if it does not exist.

            Args:
                trial_id: the lead organization study id for this trial
                metadata: a partial metadata object for trial_id

            TODO: implement metadata merging, either here or in cidc_schemas
        """
        session = app.data.driver.session

        # Look for an existing trial
        trial = session.query(TrialMetadata).filter_by(trial_id=trial_id).first()

        if trial:
            # Merge-update metadata into existing trial's metadata_json
            raise NotImplementedError("metadata updates not yet supported")
        else:
            # Create a new trial metadata record, since none exists
            app.logger.info(f"Creating new trial_metadata for trial {trial_id}")
            new_trial = TrialMetadata(trial_id=trial_id, metadata_json=metadata)
            session.add(new_trial)
            session.commit()
