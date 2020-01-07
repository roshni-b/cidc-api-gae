from io import BytesIO
from unittest.mock import MagicMock
import datetime

from cidc_api.gcloud_client import (
    grant_upload_access,
    revoke_upload_access,
    publish_upload_success,
    send_email,
    _xlsx_gcs_uri_format,
)
from cidc_api.config.settings import GOOGLE_UPLOAD_ROLE

EMAIL = "test@email.com"


def _mock_gcloud_storage(members, set_iam_policy_fn, monkeypatch):
    api_request = MagicMock()
    api_request.return_value = {
        "bindings": [{"role": GOOGLE_UPLOAD_ROLE, "members": ["rando"] + members}]
    }
    monkeypatch.setattr("google.cloud._http.JSONConnection.api_request", api_request)

    def set_iam_policy(self, policy):
        assert "rando" in policy[GOOGLE_UPLOAD_ROLE]
        set_iam_policy_fn(self, policy)

    monkeypatch.setattr(
        "google.cloud.storage.bucket.Bucket.set_iam_policy", set_iam_policy_fn
    )

    # mocking `google.cloud.storage.Client()` to not actually create a client
    monkeypatch.setattr(
        "google.cloud.client.ClientWithProject.__init__", lambda *a, **kw: None
    )


def test_grant_upload_access(monkeypatch):
    def set_iam_policy(self, policy):
        assert f"user:{EMAIL}" in policy[GOOGLE_UPLOAD_ROLE]

    _mock_gcloud_storage([], set_iam_policy, monkeypatch)

    grant_upload_access(EMAIL)


def test_revoke_upload_access(monkeypatch):
    def set_iam_policy(self, policy):
        assert f"user:{EMAIL}" not in policy[GOOGLE_UPLOAD_ROLE]

    _mock_gcloud_storage([f"user:{EMAIL}"], set_iam_policy, monkeypatch)

    revoke_upload_access(EMAIL)


def test_xlsx_gcs_uri_format(monkeypatch):

    trial = "whatever"
    template_type = "also_whatever"
    assay_type = "something_else"

    uri = _xlsx_gcs_uri_format.format(
        trial_id=trial,
        template_category=template_type,
        template_type=assay_type,
        upload_moment=datetime.datetime.now().isoformat(),
    )
    assert trial in uri
    assert template_type in uri
    assert assay_type in uri
