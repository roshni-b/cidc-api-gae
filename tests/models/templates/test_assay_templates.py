import os.path
from unittest.mock import MagicMock

from cidc_api.models import (
    File,
    HandeAssay,
    HandeUpload,
    insert_record_batch,
    Users,
    with_default_session,
)
from cidc_api.shared import auth

from .examples import EXAMPLE_DIR
from . import setup_example


def mock_get_current_user(monkeypatch):
    get_current_user = MagicMock()
    get_current_user.return_value = Users(email="user@email.com")
    monkeypatch.setattr(auth, "get_current_user", get_current_user)


def test_hande_assay(clean_db, cidc_api, monkeypatch):
    mock_get_current_user(monkeypatch)
    cidc_api = setup_example(cidc_api)

    with cidc_api.app_context():
        records = HandeAssay.read(os.path.join(EXAMPLE_DIR, "hande_assay.xlsx"))
        errors = insert_record_batch(records)
        assert len(errors) == 0, "\n".join([str(e) for e in errors])

        @with_default_session
        def check_insertion(session):
            entry = session.query(HandeUpload).first()

            assert entry is not None
            assert entry.trial_id == "test_trial"

            records = [r for r in entry.records]
            images = [i for i in entry.images]
            assert len(records) == 2, str(records)
            assert len(images) == 2, str(images)
            for i in (1, 2):
                record, image = records[i - 1], images[i - 1]
                assert record.cimac_id == f"CTTTPP1{i}1.00"
                assert record.assay_id == entry.id
                assert record.trial_id == "test_trial"
                for k in [
                    "tumor_tissue_percentage",
                    "viable_tumor_percentage",
                    "viable_stroma_percentage",
                    "necrosis_percentage",
                    "fibrosis_percentage",
                ]:
                    assert getattr(record, k) == i
                assert record.comment == f"a{'nother' if i-1 else ''} comment"

                assert (
                    image.object_url
                    == f"test_trial/hande/CTTTPP1{i}1.00/image_file.svs"
                )
                assert image.upload_id == entry.id
                assert image.trial_id == "test_trial"
                assert image.local_path == f"path/to/image{i}.svs"
                assert image.data_format == "hande_image.svs"

                assert record.image.unique_field_values() == image.unique_field_values()

        check_insertion()
