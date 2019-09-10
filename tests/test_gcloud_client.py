from io import BytesIO
from unittest.mock import MagicMock
import datetime

from cidc_api.gcloud_client import (
    grant_upload_access,
    revoke_upload_access,
    _iam_id,
    publish_upload_success,
    send_email,
    _xlsx_gcs_uri_format,
)
from cidc_api.config.settings import GOOGLE_UPLOAD_ROLE

EMAIL = "test@email.com"


class FakeBlob:
    def __init__(self, *args):
        pass


def test_grant_upload_access(monkeypatch):
    class GrantBlob(FakeBlob):
        def get_iam_policy(self):
            return {GOOGLE_UPLOAD_ROLE: set()}

        def set_iam_policy(self, policy):
            assert _iam_id(EMAIL) in policy[GOOGLE_UPLOAD_ROLE]

    monkeypatch.setattr("cidc_api.gcloud_client._get_bucket", GrantBlob)
    grant_upload_access("foo", EMAIL)


def test_revoke_upload_access(monkeypatch):
    class RevokeBlob(FakeBlob):
        def get_iam_policy(self):
            return {GOOGLE_UPLOAD_ROLE: set(EMAIL)}

        def set_iam_policy(self, policy):
            assert _iam_id(EMAIL) not in policy[GOOGLE_UPLOAD_ROLE]

    monkeypatch.setattr("cidc_api.gcloud_client._get_bucket", RevokeBlob)
    revoke_upload_access("foo", EMAIL)


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
