import json
import os

os.environ["TZ"] = "UTC"
from io import BytesIO
from unittest.mock import call, MagicMock
from datetime import datetime

from werkzeug.datastructures import FileStorage
from google.api_core.iam import Policy

from cidc_api.shared import gcloud_client
from cidc_api.config import settings
from cidc_api.shared.gcloud_client import (
    create_intake_bucket,
    grant_download_access,
    grant_lister_access,
    grant_upload_access,
    refresh_intake_access,
    revoke_all_download_access,
    revoke_download_access,
    revoke_lister_access,
    revoke_upload_access,
    upload_xlsx_to_gcs,
    upload_xlsx_to_intake_bucket,
    _build_iam_binding,
    _build_trial_upload_prefixes,
    _pseudo_blob,
    _xlsx_gcs_uri_format,
)
from cidc_api.config.settings import (
    GOOGLE_ACL_DATA_BUCKET,
    GOOGLE_INTAKE_BUCKET,
    GOOGLE_INTAKE_ROLE,
    GOOGLE_LISTER_ROLE,
    GOOGLE_UPLOAD_BUCKET,
    GOOGLE_UPLOAD_ROLE,
)

ID = 123
EMAIL = "test.user@email.com"


def _mock_gcloud_storage_client(
    monkeypatch, iam_bindings=[], set_iam_policy_fn=None
) -> MagicMock:
    """
    Mocks google.cloud.storage and google.cloud.storage.Client, returning the client
    Mocks both IAM- and ACL-related functions
    While IAM parameters are explicitly passed for background bindings and a check function,
        ACL checks are performed by checking calls to b.acl.[grant/revoke]_[role] for b in the returned mock_client.blobs
        mock_client.list_blobs returns [mock_client.blobs[0]] if prefix == "10021/wes" else mock_client.blobs

    Parameters
    ----------
    monkeypatch
        needed for mocking
    iam_bindings : List[{"role": str, "members": List[str]}] = []
        returned by [Blob/Bucket].get_iam_policy
        mocks the google return of the existing bindings on the objects
    set_iam_policy_fn : Callable = None
        single arg will be the updated IAM policy, in the form {str role: List[str member]}
        use to assert that changes have been made while also mocking google call

    Returns
    -------
    mock_client : MagicMock
        the return value mocked mocking `gcloud_client._get_storage_client`
        ACL checks are performed by checking calls to b.acl.[grant/revoke]_[role] for b in the mock_client.blobs
        mock_client.list_blobs returns [mock_client.blobs[0]] if prefix == "10021/wes" else mock_client.blobs
    """
    api_request = MagicMock()
    api_request.return_value = {"bindings": iam_bindings}
    monkeypatch.setattr(
        "google.cloud.storage.blob.Blob.get_iam_policy", lambda *a, **kw: api_request
    )

    def set_iam_policy(self, policy):
        set_iam_policy_fn(policy)

    monkeypatch.setattr("google.cloud.storage.blob.Blob.set_iam_policy", set_iam_policy)
    monkeypatch.setattr(
        "google.cloud.storage.bucket.Bucket.set_iam_policy", set_iam_policy
    )

    # mocking `google.cloud.storage.Client()` to not actually create a client
    # mock ACL-related `client.list_blobs` to return fake objects entirely
    mock_client = MagicMock()
    mock_client.blobs = [
        MagicMock(),
        MagicMock(),
    ]

    mock_client.blob_users = [
        MagicMock(),
        MagicMock(),
    ]
    mock_client.blobs[0].acl.user.return_value = mock_client.blob_users[0]
    mock_client.blobs[1].acl.user.return_value = mock_client.blob_users[1]

    def mock_list_blobs(*a, prefix: str = "", **kw):
        if prefix == "10021/wes":
            return [mock_client.blobs[0]]
        else:
            return mock_client.blobs

    mock_client.list_blobs = mock_list_blobs
    # then check calls to b.acl.[grant/revoke]_[role] for b in mock_client.blobs
    # note the return value mock_client.list_blobs depends solely on the `prefix` kwargs

    # mocking `gcloud_client._get_storage_client` to not actually create a client
    monkeypatch.setattr(
        gcloud_client, "_get_storage_client", lambda *a, **kw: mock_client
    )

    return mock_client


def test_build_trial_upload_prefixes(monkeypatch):
    fake_trial_ids = ["foo", "bar", "baz"]

    from cidc_api.models.models import TrialMetadata

    mock_list = MagicMock()
    mock_list.return_value = [MagicMock(trial_id=t) for t in fake_trial_ids]
    monkeypatch.setattr(TrialMetadata, "list", mock_list)

    assert set(_build_trial_upload_prefixes(None, "rna_bam")) == set(
        f"{t}/rna" for t in fake_trial_ids
    )
    assert _build_trial_upload_prefixes("foo", None) == ["foo"]
    assert _build_trial_upload_prefixes("foo", "rna_bam") == ["foo/rna"]


def test_grant_lister_access(monkeypatch):
    """Check that grant_lister_access adds policy bindings as expected"""

    def set_iam_policy(policy):
        assert len(policy.bindings) == 2, str(policy.bindings)
        assert all(b["role"] == GOOGLE_LISTER_ROLE for b in policy.bindings)
        assert any("user:rando" in b["members"] for b in policy.bindings)
        assert any(f"user:{EMAIL}" in b["members"] for b in policy.bindings)

    _mock_gcloud_storage_client(
        monkeypatch,
        [
            {"role": GOOGLE_LISTER_ROLE, "members": ["user:rando"]},
            {"role": GOOGLE_LISTER_ROLE, "members": [f"user:{EMAIL}"]},
        ],
        set_iam_policy,
    )

    grant_lister_access(EMAIL)


def test_revoke_lister_access(monkeypatch):
    """Check that grant_lister_access adds policy bindings as expected"""

    def set_iam_policy(policy):
        assert len(policy.bindings) == 1
        assert all(b["role"] == GOOGLE_LISTER_ROLE for b in policy.bindings)
        assert any("user:rando" in b["members"] for b in policy.bindings)
        assert all(f"user:{EMAIL}" not in b["members"] for b in policy.bindings)

    _mock_gcloud_storage_client(
        monkeypatch,
        [
            {"role": GOOGLE_LISTER_ROLE, "members": ["user:rando"]},
            {"role": GOOGLE_LISTER_ROLE, "members": [f"user:{EMAIL}"]},
        ],
        set_iam_policy,
    )

    revoke_lister_access(EMAIL)


def test_grant_upload_access(monkeypatch):
    def set_iam_policy(policy):
        assert f"user:rando" in policy[GOOGLE_UPLOAD_ROLE]
        assert f"user:{EMAIL}" in policy[GOOGLE_UPLOAD_ROLE]

    _mock_gcloud_storage_client(
        monkeypatch,
        [{"role": GOOGLE_UPLOAD_ROLE, "members": ["user:rando"]}],
        set_iam_policy,
    )

    grant_upload_access(EMAIL)


def test_revoke_upload_access(monkeypatch):
    def set_iam_policy(policy):
        assert f"user:rando" in policy[GOOGLE_UPLOAD_ROLE]
        assert f"user:{EMAIL}" not in policy[GOOGLE_UPLOAD_ROLE]

    _mock_gcloud_storage_client(
        monkeypatch,
        [{"role": GOOGLE_UPLOAD_ROLE, "members": ["user:rando", f"user:{EMAIL}"]}],
        set_iam_policy,
    )

    revoke_upload_access(EMAIL)


def test_create_intake_bucket(monkeypatch):
    policy = Policy()
    bucket = MagicMock()
    bucket.exists.return_value = False
    bucket.get_iam_policy.return_value = policy
    storage_client = MagicMock()
    storage_client.bucket.return_value = bucket
    storage_client.create_bucket.return_value = bucket

    monkeypatch.setattr(
        "cidc_api.shared.gcloud_client._get_storage_client", lambda: storage_client
    )

    def set_iam_policy(policy):
        assert len(policy.bindings) == 1, str(policy.bindings)
        assert policy.bindings[0]["role"] == GOOGLE_INTAKE_ROLE
        assert policy.bindings[0]["members"] == {f"user:{EMAIL}"}

    create_intake_bucket(EMAIL)
    _mock_gcloud_storage_client(
        monkeypatch,
        [
            {"role": GOOGLE_LISTER_ROLE, "members": ["user:rando"]},
            {"role": GOOGLE_LISTER_ROLE, "members": [f"user:{EMAIL}"]},
        ],
        set_iam_policy,
    )

    # Bucket name should have structure:
    # <intake bucket prefix>-<10 character email hash>
    name, hash = storage_client.bucket.call_args[0][0].rsplit("-", 1)
    assert name == GOOGLE_INTAKE_BUCKET
    assert len(hash) == 10 and EMAIL not in hash

    # The bucket gets created and permissions get granted
    storage_client.create_bucket.assert_called_once_with(bucket)
    bucket.get_iam_policy.assert_called_once()
    bucket.set_iam_policy.assert_called_once_with(policy)

    # If the bucket already exists, it doesn't get re-created
    storage_client.create_bucket.reset_mock()
    bucket.exists.return_value = True
    create_intake_bucket(EMAIL)
    storage_client.create_bucket.assert_not_called()


def test_refresh_intake_access(monkeypatch):
    _mock_gcloud_storage_client(
        monkeypatch,
        _build_iam_binding(GOOGLE_INTAKE_BUCKET, GOOGLE_INTAKE_ROLE, EMAIL),
        lambda i: i,
    )

    grant_gcs_access = MagicMock()
    monkeypatch.setattr(
        "cidc_api.shared.gcloud_client.grant_gcs_access", grant_gcs_access
    )

    refresh_intake_access(EMAIL)
    args, kwargs = grant_gcs_access.call_args_list[0]
    assert args[0].name.startswith(GOOGLE_INTAKE_BUCKET)
    assert args[1:] == (GOOGLE_INTAKE_ROLE, EMAIL)
    assert "iam" in kwargs and kwargs["iam"]


def test_grant_download_access(monkeypatch):
    """Check that grant_download_access makes ACL calls as expected"""
    client = _mock_gcloud_storage_client(monkeypatch)
    grant_download_access(EMAIL, "10021", "wes_analysis")
    client.blobs[0].acl.user.assert_called_once_with(EMAIL)
    client.blob_users[0].grant_read.assert_called_once()
    client.blobs[0].acl.save.assert_called_once()
    client.blobs[1].acl.user.assert_not_called()
    client.blobs[1].acl.save.assert_not_called()


def test_revoke_download_access(monkeypatch):
    """Check that revoke_download_access makes ACL calls as expected"""
    client = _mock_gcloud_storage_client(monkeypatch)
    revoke_download_access(EMAIL, "10021", "wes_analysis")
    client.blobs[0].acl.user.assert_called_once_with(EMAIL)
    client.blob_users[0].revoke_owner.assert_called_once()
    client.blob_users[0].revoke_reader.assert_called_once()
    client.blob_users[0].revoke_writer.assert_called_once()
    client.blobs[0].acl.save.assert_called_once()
    client.blobs[1].acl.user.assert_not_called()
    client.blobs[1].acl.save.assert_not_called()


def test_revoke_all_download_access(monkeypatch):
    """Check that revoke_all_download_access makes ACL calls as expected against ALL blobs"""
    client = _mock_gcloud_storage_client(monkeypatch)
    revoke_all_download_access(EMAIL)
    for blob, blob_user in zip(client.blobs, client.blob_users):
        blob.acl.user.assert_called_once_with(EMAIL)
        blob_user.revoke_owner.assert_called_once()
        blob_user.revoke_reader.assert_called_once()
        blob_user.revoke_writer.assert_called_once()
        blob.acl.save.assert_called_once()


def test_xlsx_gcs_uri_format(monkeypatch):

    trial = "whatever"
    template_type = "also_whatever"
    assay_type = "something_else"

    uri = _xlsx_gcs_uri_format.format(
        trial_id=trial,
        template_category=template_type,
        template_type=assay_type,
        upload_moment=datetime.now().isoformat(),
    )
    assert trial in uri
    assert template_type in uri
    assert assay_type in uri


def test_upload_xlsx_to_gcs(monkeypatch):
    trial_id = "test-trial"
    upload_category = "assays"
    upload_type = "olink"
    upload_moment = datetime.now()
    open_file = BytesIO(b"foobar")
    expected_name = (
        f"{trial_id}/xlsx/{upload_category}/{upload_type}/{upload_moment}.xlsx"
    )

    # upload_xlsx_to_gcs should return a `_pseudo_blob` when ENV = "dev"
    res = upload_xlsx_to_gcs(
        trial_id, upload_category, upload_type, open_file, upload_moment
    )
    assert type(res) == _pseudo_blob
    assert res.name == expected_name
    assert res.time_created == upload_moment

    # upload_xlsx_to_gcs should call GCS api when ENV = "prod"
    monkeypatch.setattr(gcloud_client, "ENV", "prod")
    _get_bucket = MagicMock()
    _get_bucket.return_value = bucket = MagicMock()
    bucket.blob.return_value = blob = MagicMock()
    bucket.copy_blob.return_value = copied_blob = MagicMock()
    monkeypatch.setattr("cidc_api.shared.gcloud_client._get_bucket", _get_bucket)
    res = upload_xlsx_to_gcs(
        trial_id, upload_category, upload_type, open_file, upload_moment
    )
    assert res == copied_blob
    assert call(GOOGLE_UPLOAD_BUCKET) in _get_bucket.call_args_list
    assert call(GOOGLE_ACL_DATA_BUCKET) in _get_bucket.call_args_list
    bucket.blob.assert_called_once_with(expected_name)
    blob.upload_from_file.assert_called_once_with(open_file)
    bucket.copy_blob.assert_called_once_with(blob, bucket)


def test_upload_xlsx_to_intake_bucket(monkeypatch):
    trial_id = "test-trial"
    assay_type = "wes"
    xlsx = FileStorage(filename="metadata.xlsx")

    _get_bucket = MagicMock()
    _get_bucket.return_value = bucket = MagicMock()
    bucket.blob.return_value = blob = MagicMock()
    monkeypatch.setattr("cidc_api.shared.gcloud_client._get_bucket", _get_bucket)

    url = upload_xlsx_to_intake_bucket(EMAIL, trial_id, assay_type, xlsx)
    blob.upload_from_file.assert_called_once()
    assert url.startswith(
        "https://console.cloud.google.com/storage/browser/_details/cidc-intake-staging-"
    )
    assert f"/{trial_id}/{assay_type}" in url
    assert url.endswith(".xlsx")


def test_get_signed_url(monkeypatch):
    storage_client = MagicMock()
    storage_client.get_bucket.return_value = bucket = MagicMock()
    bucket.blob.return_value = blob = MagicMock()
    blob.generate_signed_url = lambda **kwargs: kwargs["response_disposition"]

    monkeypatch.setattr(
        "cidc_api.shared.gcloud_client._get_storage_client", lambda: storage_client
    )

    object_name = "path/to/obj"
    signed_url = gcloud_client.get_signed_url(object_name)
    assert signed_url == 'attachment; filename="path_to_obj"'


def test_encode_and_publish(monkeypatch):
    pubsub = MagicMock()
    pubsub.PublisherClient.return_value = pubsub_client = MagicMock()
    pubsub_client.topic_path = lambda proj, top: top
    pubsub_client.publish.return_value = report = MagicMock()
    monkeypatch.setattr(gcloud_client, "pubsub", pubsub)

    # Make sure the ENV = "prod" case publishes
    monkeypatch.setattr(gcloud_client, "ENV", "prod")
    topic = "some-topic"
    content = "some message"
    res = gcloud_client._encode_and_publish(content, topic)
    assert res == report
    pubsub_client.publish.assert_called_once_with(topic, data=bytes(content, "utf-8"))


def mock_encode_and_publish(monkeypatch):
    _encode_and_publish = MagicMock()
    monkeypatch.setattr(
        "cidc_api.shared.gcloud_client._encode_and_publish", _encode_and_publish
    )
    return _encode_and_publish


def test_publish_upload_success(monkeypatch):
    _encode_and_publish = mock_encode_and_publish(monkeypatch)
    gcloud_client.publish_upload_success("foo")
    _encode_and_publish.assert_called_with("foo", settings.GOOGLE_UPLOAD_TOPIC)


def test_publish_patient_sample_update(monkeypatch):
    _encode_and_publish = mock_encode_and_publish(monkeypatch)
    gcloud_client.publish_patient_sample_update("foo")
    _encode_and_publish.assert_called_with("foo", settings.GOOGLE_PATIENT_SAMPLE_TOPIC)


def test_publish_artifact_upload(monkeypatch):
    _encode_and_publish = mock_encode_and_publish(monkeypatch)
    gcloud_client.publish_artifact_upload("foo")
    _encode_and_publish.assert_called_with("foo", settings.GOOGLE_ARTIFACT_UPLOAD_TOPIC)


def test_send_email(monkeypatch):
    _encode_and_publish = mock_encode_and_publish(monkeypatch)

    to_emails = ["test@example.com"]
    subject = "test subject"
    html_content = "<div>test html<div>"
    kwargs = {"kwarg1": "foo", "kwarg2": "bar"}
    expected_json = json.dumps(
        {
            "to_emails": to_emails,
            "subject": subject,
            "html_content": html_content,
            **kwargs,
        }
    )

    # If ENV = "dev", no emails are sent
    gcloud_client.send_email(to_emails, subject, html_content, **kwargs)
    _encode_and_publish.assert_not_called()

    # Check ENV = "prod" behavior
    monkeypatch.setattr(gcloud_client, "ENV", "prod")
    monkeypatch.setattr(gcloud_client, "TESTING", False)
    gcloud_client.send_email(to_emails, subject, html_content, **kwargs)
    _encode_and_publish.assert_called_with(expected_json, settings.GOOGLE_EMAILS_TOPIC)
