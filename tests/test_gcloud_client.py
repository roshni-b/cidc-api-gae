from gcloud_client import grant_upload_access, revoke_upload_access, _iam_id
from settings import GOOGLE_UPLOAD_ROLE

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

    monkeypatch.setattr("gcloud_client._get_bucket", GrantBlob)
    grant_upload_access("foo", EMAIL)


def test_revoke_upload_access(monkeypatch):
    class RevokeBlob(FakeBlob):
        def get_iam_policy(self):
            return {GOOGLE_UPLOAD_ROLE: set(EMAIL)}

        def set_iam_policy(self, policy):
            assert _iam_id(EMAIL) not in policy[GOOGLE_UPLOAD_ROLE]

    monkeypatch.setattr("gcloud_client._get_bucket", RevokeBlob)
    revoke_upload_access("foo", EMAIL)
