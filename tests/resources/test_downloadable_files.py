from datetime import datetime
from typing import Tuple

from cidc_api.models import (
    Users,
    DownloadableFiles,
    TrialMetadata,
    Permissions,
    CIDCRole,
)
from cidc_api.config.settings import GOOGLE_DATA_BUCKET

from ..utils import mock_current_user, make_admin, make_role, mock_gcloud_client


def setup_user(cidc_api, monkeypatch) -> int:
    # this is necessary for adding/removing permissions from this user
    # without trying to contact GCP
    mock_gcloud_client(monkeypatch)

    current_user = Users(
        email="test@email.com",
        role=CIDCRole.CIMAC_USER.value,
        approval_date=datetime.now(),
    )
    mock_current_user(current_user, monkeypatch)

    with cidc_api.app_context():
        current_user.insert()
        return current_user.id


trial_id = "test-trial"
upload_types = ["wes", "cytof"]


def setup_downloadable_files(cidc_api) -> Tuple[int, int]:
    """Insert two downloadable files into the database."""
    trial_id = "test-trial"
    metadata_json = {
        "protocol_identifier": trial_id,
        "allowed_collection_event_names": [],
        "allowed_cohort_names": [],
        "participants": [],
    }
    trial = TrialMetadata(trial_id=trial_id, metadata_json=metadata_json)

    def make_file(object_url, upload_type, facet_group) -> DownloadableFiles:
        return DownloadableFiles(
            trial_id=trial_id,
            upload_type=upload_type,
            object_url=object_url,
            data_format="",
            facet_group=facet_group,
            uploaded_timestamp=datetime.now(),
            file_size_bytes=0,
            file_name="",
        )

    wes_file = make_file(
        f"{trial_id}/wes/.../reads_123.bam", "wes", "/wes/r1_.fastq.gz"
    )
    cytof_file = make_file(
        f"{trial_id}/cytof/.../analysis.zip", "cytof", "/cytof_analysis/analysis.zip"
    )

    with cidc_api.app_context():
        trial.insert()
        wes_file.insert()
        cytof_file.insert()

        return wes_file.id, cytof_file.id


def test_list_downloadable_files(cidc_api, clean_db, monkeypatch):
    """Check that getting a list of files works as expected"""
    user_id = setup_user(cidc_api, monkeypatch)
    file_id_1, file_id_2 = setup_downloadable_files(cidc_api)

    client = cidc_api.test_client()

    # Non-admins can't get files they don't have permissions for
    res = client.get("/downloadable_files")
    assert res.status_code == 200
    assert len(res.json["_items"]) == 0
    assert res.json["_meta"]["total"] == 0

    # Give the user one permission
    with cidc_api.app_context():
        perm = Permissions(
            granted_to_user=user_id, trial_id=trial_id, upload_type=upload_types[0]
        )
        perm.insert()

    # Non-admins can view files for which they have permission
    res = client.get("/downloadable_files")
    assert res.status_code == 200
    assert len(res.json["_items"]) == 1
    assert res.json["_meta"]["total"] == 1
    assert res.json["_items"][0]["id"] == file_id_1

    # Non-admin filter queries exclude files they aren't allowed to view
    res = client.get(f"/downloadable_files?facets=Assay Type|CyTOF|Analysis Results")
    assert res.status_code == 200
    assert len(res.json["_items"]) == 0
    assert res.json["_meta"]["total"] == 0

    # Admins and NCI biobank users can view all files regardless of their permissions
    for role in [CIDCRole.ADMIN.value, CIDCRole.NCI_BIOBANK_USER.value]:
        make_role(user_id, role, cidc_api)
        res = client.get("/downloadable_files")
        assert res.status_code == 200
        assert len(res.json["_items"]) == 2
        assert res.json["_meta"]["total"] == 2
        assert set([f["id"] for f in res.json["_items"]]) == set([file_id_1, file_id_2])

        # Admin filter queries include any files that fit the criteria
        res = client.get(
            f"/downloadable_files?facets=Assay Type|CyTOF|Analysis Results"
        )
        assert res.status_code == 200
        assert len(res.json["_items"]) == 1
        assert res.json["_meta"]["total"] == 1
        assert res.json["_items"][0]["id"] == file_id_2

    # Make sure it's possible to sort by file extension
    res = client.get(f"/downloadable_files?sort_field=file_ext&sort_direction=asc")
    assert res.status_code == 200
    assert [f["file_ext"] for f in res.json["_items"]] == ["bam", "zip"]

    # Make sure it's possible to sort by data category
    res = client.get(f"/downloadable_files?sort_field=data_category&sort_direction=asc")
    assert res.status_code == 200
    assert [f["data_category"] for f in res.json["_items"]] == [
        "CyTOF|Analysis Results",
        "WES|Source",
    ]


def test_get_downloadable_file(cidc_api, clean_db, monkeypatch):
    """Check that getting a single file works as expected."""
    user_id = setup_user(cidc_api, monkeypatch)
    file_id_1, file_id_2 = setup_downloadable_files(cidc_api)

    client = cidc_api.test_client()

    # Non-admins get 404s for single files they don't have permision to view
    res = client.get(f"/downloadable_files/{file_id_1}")
    assert res.status_code == 404

    # Give the user one permission
    with cidc_api.app_context():
        perm = Permissions(
            granted_to_user=user_id, trial_id=trial_id, upload_type=upload_types[0]
        )
        perm.insert()

    # Non-admins can get single files that they have permision to view
    res = client.get(f"/downloadable_files/{file_id_1}")
    assert res.status_code == 200
    assert res.json["id"] == file_id_1

    # Admins can get any file regardless of permissions
    make_admin(user_id, cidc_api)
    res = client.get(f"/downloadable_files/{file_id_2}")
    assert res.status_code == 200
    assert res.json["id"] == file_id_2

    # Non-existent files yield 404
    res = client.get(f"/downloadable_files/123212321")
    assert res.status_code == 404


def test_get_filelist(cidc_api, clean_db, monkeypatch):
    """Check that getting a filelist.tsv works as expected"""
    user_id = setup_user(cidc_api, monkeypatch)
    file_id_1, file_id_2 = setup_downloadable_files(cidc_api)

    client = cidc_api.test_client()

    # The file_ids query param must be provided
    res = client.get("/downloadable_files/filelist")
    assert res.status_code == 422

    url = f"/downloadable_files/filelist?file_ids={file_id_1},{file_id_2}"

    # User has no permissions, so no files should be found
    res = client.get(url)
    assert res.status_code == 404

    # Give the user one permission
    with cidc_api.app_context():
        perm = Permissions(
            granted_to_user=user_id, trial_id=trial_id, upload_type=upload_types[0]
        )
        perm.insert()

    # User has one permission, so the filelist should contain a single file
    res = client.get(url)
    assert res.status_code == 200
    assert "text/tsv" in res.headers["Content-Type"]
    assert "filename=filelist.tsv" in res.headers["Content-Disposition"]
    assert res.data.decode("utf-8") == (
        f"gs://{GOOGLE_DATA_BUCKET}/{trial_id}/wes/.../reads_123.bam\t{trial_id}_wes_..._reads_123.bam\n"
    )

    # Admins can get a filelist containing all files
    make_admin(user_id, cidc_api)
    res = client.get(url)
    assert res.status_code == 200
    assert res.data.decode("utf-8") == (
        f"gs://{GOOGLE_DATA_BUCKET}/{trial_id}/wes/.../reads_123.bam\t{trial_id}_wes_..._reads_123.bam\n"
        f"gs://{GOOGLE_DATA_BUCKET}/{trial_id}/cytof/.../analysis.zip\t{trial_id}_cytof_..._analysis.zip\n"
    )

    # Filelists don't get truncated (i.e., paginated)
    new_ids = range(1000, 1300)
    with cidc_api.app_context():
        for id in new_ids:
            DownloadableFiles(
                id=id,
                trial_id=trial_id,
                object_url=str(id),
                upload_type="",
                file_name="",
                data_format="",
                file_size_bytes=0,
                uploaded_timestamp=datetime.now(),
            ).insert()

    res = client.get(f"{url},{','.join([str(id) for id in new_ids])}")
    assert res.status_code == 200
    # newly inserted files + already inserted files + EOF newline
    assert len(res.data.decode("utf-8").split("\n")) == len(new_ids) + 2 + 1


def test_get_filter_facets(cidc_api, clean_db, monkeypatch):
    """Check that getting filter facets works as expected"""
    user_id = setup_user(cidc_api, monkeypatch)
    setup_downloadable_files(cidc_api)

    client = cidc_api.test_client()

    res = client.get("/downloadable_files/filter_facets")
    assert res.status_code == 200
    assert res.json["trial_ids"] == [trial_id]
    assert isinstance(res.json["facets"], dict)


def test_get_download_url(cidc_api, clean_db, monkeypatch):
    """Check that generating a GCS signed URL works as expected"""
    user_id = setup_user(cidc_api, monkeypatch)
    file_id, _ = setup_downloadable_files(cidc_api)

    client = cidc_api.test_client()

    # A query missing the required parameters should yield 422
    res = client.get("/downloadable_files/download_url")
    assert res.status_code == 422
    assert res.json["_error"]["message"]["query"]["id"] == [
        "Missing data for required field."
    ]

    # A missing file should yield 404
    res = client.get("/downloadable_files/download_url?id=123212321")
    assert res.status_code == 404

    # No permission should also yield 404
    res = client.get(f"/downloadable_files/download_url?id={file_id}")
    assert res.status_code == 404

    with cidc_api.app_context():
        perm = Permissions(
            granted_to_user=user_id, trial_id=trial_id, upload_type=upload_types[0]
        )
        perm.insert()

    test_url = "foo"
    monkeypatch.setattr(
        "cidc_api.shared.gcloud_client.get_signed_url", lambda *args: test_url
    )

    res = client.get(f"/downloadable_files/download_url?id={file_id}")
    assert res.status_code == 200
    assert res.json == test_url
