from flask import current_app as app
from sqlalchemy import Column, DateTime, Integer, String, Enum, func
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

    @classmethod
    def find_or_create(cls, email: str):
        """
            Create a new record for a user if one doesn't exist
            for the given email.
        """
        session = app.data.driver.session
        if not session.query(Users).filter_by(email=email).first():
            app.logger.info(f"Creating new user with email {email}")
            session.add(Users(email=email))
            session.commit()
