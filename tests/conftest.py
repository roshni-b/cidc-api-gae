from cidc_api.models.templates.assay_metadata import WESUpload
import os
import importlib

import pytest
from webdriver_manager.chrome import ChromeDriverManager
from flask import Flask

# The below imports depend on these environment variables,
# so set it before importing them.
os.environ["TESTING"] = "True"
os.environ["DEBUG"] = "False"

from cidc_api.app import app
from cidc_api.models import (
    UploadJobs,
    Users,
    DownloadableFiles,
    TrialMetadata,
    Permissions,
    Aliquot,
    ClinicalTrial,
    Cohort,
    CollectionEvent,
    Participant,
    Sample,
    Shipment,
    HandeImage,
    HandeRecord,
    HandeUpload,
    File,
    Upload,
    NGSUpload,
    NGSAssayFiles,
    WESUpload,
    WESRecord,
)

# Install the Chrome web driver and add it to the PATH env variable
chromedriver_dir = os.path.dirname(ChromeDriverManager().install())
os.environ["PATH"] = f"{os.environ['PATH']}:{chromedriver_dir}"


@pytest.fixture
def empty_app():
    return Flask(__name__)


@pytest.fixture
def cidc_api():
    """An instance of the CIDC API"""
    return app


@pytest.fixture
def clean_cidc_api():
    """An instance of the CIDC API that hasn't yet handled any requests."""
    import cidc_api.app

    importlib.reload(cidc_api.app)

    return cidc_api.app.app


@pytest.fixture
def clean_db(cidc_api):
    """Provide a clean test database session"""
    with cidc_api.app_context():
        session = cidc_api.extensions["sqlalchemy"].db.session
        with session.no_autoflush:
            session.query(UploadJobs).delete()
            session.query(NGSAssayFiles)  # before Files and NGSUpload
            session.query(HandeRecord).delete()  # before HandeUpload and HandeImage
            session.query(HandeImage).delete()  # before File
            session.query(File).delete()
            session.query(WESRecord).delete()  # before WESUpload
            session.query(WESUpload).delete()  # before NGSUpload

            session.query(NGSUpload).delete()  # after WESUpload and NGSAssayFiles
            session.query(HandeUpload).delete()  # after NGSUpload, before Upload
            session.query(Upload).delete()
            session.query(Aliquot).delete()
            session.query(Sample).delete()
            session.query(Shipment).delete()
            session.query(Participant).delete()
            session.query(CollectionEvent).delete()
            session.query(Cohort).delete()
            session.query(ClinicalTrial).delete()
            session.query(Users).delete()
            session.query(DownloadableFiles).delete()
            session.query(TrialMetadata).delete()
            session.query(Permissions).delete()
            session.commit()

    return session
