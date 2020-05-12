import json
from io import BytesIO
from unittest.mock import MagicMock, call
from datetime import datetime

from cidc_api.shared import gcloud_client
from cidc_api.config import settings
from cidc_api.shared.gcloud_client import (
    grant_upload_access,
    revoke_upload_access,
    publish_upload_success,
    send_email,
    _xlsx_gcs_uri_format,
    upload_xlsx_to_gcs,
    _pseudo_blob,
)
from cidc_api.config.settings import (
    GOOGLE_UPLOAD_ROLE,
    GOOGLE_UPLOAD_BUCKET,
    GOOGLE_DATA_BUCKET,
)

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
    assert call(GOOGLE_DATA_BUCKET) in _get_bucket.call_args_list
    bucket.blob.assert_called_once_with(expected_name)
    blob.upload_from_file.assert_called_once_with(open_file)
    bucket.copy_blob.assert_called_once_with(blob, bucket)


def test_get_signed_url(monkeypatch):
    storage = MagicMock()
    storage.Client.return_value = storage_client = MagicMock()
    storage_client.get_bucket.return_value = bucket = MagicMock()
    bucket.blob.return_value = blob = MagicMock()
    blob.generate_signed_url = lambda **kwargs: kwargs["response_disposition"]
    monkeypatch.setattr(gcloud_client, "storage", storage)

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
