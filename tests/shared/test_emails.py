from io import BytesIO
from unittest.mock import MagicMock

from werkzeug.datastructures import FileStorage

from cidc_api.models import Users, UploadJobs
from cidc_api.shared.emails import (
    confirm_account_approval,
    new_user_registration,
    new_upload_alert,
    intake_metadata,
    CIDC_MAILING_LIST,
)

user = Users(email="foo@bar.com", first_n="john", last_n="doe")


def test_new_user_registration():
    email = new_user_registration(user.email)
    assert user.email in email["html_content"]
    assert email["to_emails"] == [CIDC_MAILING_LIST]
    assert "New User" in email["subject"]
    assert "new user" in email["html_content"]


def test_confirm_account_approval():
    email = confirm_account_approval(user)
    assert user.first_n in email["html_content"]
    assert email["to_emails"] == [user.email]
    assert "Approval" in email["subject"]
    assert "has been approved" in email["html_content"]


def test_new_upload_alert(monkeypatch):
    vals = {"id": 1, "trial_id": "foo", "uploader_email": "test@email.com"}

    gen_confs = MagicMock()
    gen_confs.side_effect = (
        lambda ct, patch, template_type, bucket: {"attach.file": "content"}
        if "wes" in template_type
        else {}
    )
    monkeypatch.setattr(
        "cidc_api.shared.emails.generate_analysis_configs_from_upload_patch", gen_confs
    )

    for upload, full_ct, expected_att in [
        (
            UploadJobs(
                **vals, upload_type="wes_bam", metadata_patch={"assays": {"wes": []}}
            ),
            {"assays": {"wes": []}},
            [
                {
                    "content": "Y29udGVudA==",  # "content" base64 encoded
                    "filename": "attach.file",
                    "type": "application/yaml",
                }
            ],
        ),
        (UploadJobs(**vals, upload_type="pbmc", metadata_patch={}), {}, None),
    ]:
        email = new_upload_alert(upload, full_ct)
        assert "UPLOAD SUCCESS" in email["subject"]
        assert email["to_emails"] == [CIDC_MAILING_LIST]
        for val in vals.values():
            assert str(val) in email["html_content"]

        assert gen_confs.called_once()

        assert email.get("attachments") == expected_att


def test_intake_metadata():
    user = Users(
        email="test@email.com",
        contact_email="test@contactemail.com",
        first_n="foo",
        last_n="bar",
    )
    trial_id = "10021"
    assay_type = "wes"
    description = "a test description of this metadata"
    xlsx_uri = "gs://fake/gcs/uri"

    email = intake_metadata(user, trial_id, assay_type, description, xlsx_uri)
    assert email["to_emails"] == [CIDC_MAILING_LIST]
    assert f"{user.first_n} {user.last_n}" in email["html_content"]
    assert f"{user.email}" in email["html_content"]
    assert f"{user.contact_email}" in email["html_content"]
    assert f"{description}" in email["html_content"]
    assert f"{xlsx_uri}" in email["html_content"]
