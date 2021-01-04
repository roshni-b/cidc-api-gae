import json
from io import BytesIO
from unittest.mock import MagicMock, call
from datetime import datetime

import pytest

from cidc_api.shared import gcloud_client
from cidc_api.config import settings
from cidc_api.shared.gcloud_client import (
    grant_intake_access,
    grant_upload_access,
    list_intake_access,
    refresh_intake_access,
    revoke_intake_access,
    revoke_upload_access,
    grant_download_access,
    revoke_download_access,
    revoke_all_download_access,
    _xlsx_gcs_uri_format,
    upload_xlsx_to_gcs,
    _pseudo_blob,
    _build_binding_with_expiry,
)
from cidc_api.config.settings import (
    GOOGLE_INTAKE_BUCKET,
    GOOGLE_UPLOAD_ROLE,
    GOOGLE_UPLOAD_BUCKET,
    GOOGLE_DATA_BUCKET,
    GOOGLE_DOWNLOAD_ROLE,
)

ID = 123
EMAIL = "test.user@email.com"


def _mock_gcloud_storage(bindings, set_iam_policy_fn, monkeypatch):
    api_request = MagicMock()
    api_request.return_value = {"bindings": bindings}
    monkeypatch.setattr("google.cloud._http.JSONConnection.api_request", api_request)

    def set_iam_policy(self, policy):
        set_iam_policy_fn(policy)

    monkeypatch.setattr(
        "google.cloud.storage.bucket.Bucket.set_iam_policy", set_iam_policy
    )

    # mocking `google.cloud.storage.Client()` to not actually create a client
    monkeypatch.setattr(
        "google.cloud.client.ClientWithProject.__init__", lambda *a, **kw: None
    )


def test_grant_upload_access(monkeypatch):
    def set_iam_policy(policy):
        assert f"user:rando" in policy[GOOGLE_UPLOAD_ROLE]
        assert f"user:{EMAIL}" in policy[GOOGLE_UPLOAD_ROLE]

    _mock_gcloud_storage(
        [{"role": GOOGLE_UPLOAD_ROLE, "members": ["user:rando"]}],
        set_iam_policy,
        monkeypatch,
    )

    grant_upload_access(EMAIL)


def test_revoke_upload_access(monkeypatch):
    def set_iam_policy(policy):
        assert f"user:rando" in policy[GOOGLE_UPLOAD_ROLE]
        assert f"user:{EMAIL}" not in policy[GOOGLE_UPLOAD_ROLE]

    _mock_gcloud_storage(
        [{"role": GOOGLE_UPLOAD_ROLE, "members": ["user:rando", f"user:{EMAIL}"]}],
        set_iam_policy,
        monkeypatch,
    )

    revoke_upload_access(EMAIL)


def test_grant_intake_access(monkeypatch):
    grant_gcs_access = MagicMock()
    monkeypatch.setattr(
        "cidc_api.shared.gcloud_client.grant_conditional_gcs_access", grant_gcs_access
    )

    grant_intake_access(123, "test.user@email.com", "test-trial", "upload-type")

    grant_gcs_access.assert_called_once_with(
        GOOGLE_INTAKE_BUCKET,
        "test-trial/upload-type/testuser-123",
        GOOGLE_UPLOAD_ROLE,
        "test.user@email.com",
    )


def test_revoke_intake_access(monkeypatch):
    revoke_gcs_access = MagicMock()
    monkeypatch.setattr(
        "cidc_api.shared.gcloud_client.revoke_conditional_gcs_access", revoke_gcs_access
    )

    revoke_intake_access(123, "test.user@email.com", "test-trial", "upload-type")

    revoke_gcs_access.assert_called_once_with(
        GOOGLE_INTAKE_BUCKET,
        "test-trial/upload-type/testuser-123",
        GOOGLE_UPLOAD_ROLE,
        "test.user@email.com",
    )


@pytest.fixture
def trial_ids_upload_types():
    return [
        ("test-trial-1", "upload-type-1"),
        ("test-trial-2", "upload-type-2"),
        ("test-trial-3", "upload-type-3"),
    ]


@pytest.fixture
def intake_bindings(trial_ids_upload_types):
    return [
        _build_binding_with_expiry(
            GOOGLE_INTAKE_BUCKET,
            f"{trial_id}/{upload_type}/testuser-{ID}",
            GOOGLE_UPLOAD_ROLE,
            EMAIL,
        )
        for trial_id, upload_type in trial_ids_upload_types
    ]


def test_list_intake_access(intake_bindings, trial_ids_upload_types, monkeypatch):
    _mock_gcloud_storage(intake_bindings, lambda i: i, monkeypatch)

    uris = list_intake_access(EMAIL)
    assert uris == [
        f"gs://{GOOGLE_INTAKE_BUCKET}/{t}/{u}/testuser-{ID}"
        for t, u in trial_ids_upload_types
    ]


def test_refresh_intake_access(intake_bindings, trial_ids_upload_types, monkeypatch):
    _mock_gcloud_storage(intake_bindings, lambda i: i, monkeypatch)

    grant_intake_access_mock = MagicMock()
    monkeypatch.setattr(
        "cidc_api.shared.gcloud_client.grant_intake_access", grant_intake_access_mock
    )

    refresh_intake_access(ID, EMAIL)
    assert grant_intake_access_mock.call_args_list == [
        call(ID, EMAIL, t, u) for t, u in trial_ids_upload_types
    ]


def test_grant_download_access(monkeypatch):
    """Check that grant_download_access adds policy bindings as expected"""
    bindings = [
        # Role without a condition
        {"role": "some-other-role", "members": {f"user:JohnDoe"}}
    ]

    # Check simple binding creation
    def set_iam_policy(policy):
        bindings = policy.bindings
        assert len(bindings) == 1
        [binding] = bindings
        assert binding["members"] == {f"user:{EMAIL}"}
        assert binding["role"] == GOOGLE_DOWNLOAD_ROLE
        condition = binding["condition"]
        assert f"{GOOGLE_DOWNLOAD_ROLE} access on 10021/wes until" in condition["title"]
        assert "updated by the CIDC API" in condition["description"]
        assert "10021/wes" in condition["expression"]

    _mock_gcloud_storage([], set_iam_policy, monkeypatch)
    grant_download_access(EMAIL, "10021", "wes_analysis")

    matching_prefix = "10021/wes"
    matching_binding = _build_binding_with_expiry(
        GOOGLE_DATA_BUCKET, matching_prefix, GOOGLE_DOWNLOAD_ROLE, EMAIL
    )

    def set_iam_policy(policy):
        bindings = policy.bindings
        assert len(bindings) == 2
        assert any(
            matching_prefix
            in binding.get("condition", {}).get("expression", {})  # prefixes match
            and binding != matching_binding  # but TTL has changed
            for binding in bindings
        )

    # Check permission regranting - TTL should be updated, but download prefix should be unchanged
    _mock_gcloud_storage([matching_binding] + bindings, set_iam_policy, monkeypatch)
    grant_download_access(EMAIL, "10021", "wes_analysis")

    # Check adding a second binding
    def set_iam_policy(policy):
        bindings = policy.bindings
        assert len(bindings) == 3
        assert matching_binding in policy.bindings
        assert any(
            "10021/participants" in binding["condition"]["expression"]
            for binding in policy.bindings
            if "condition" in binding
        )

    _mock_gcloud_storage([matching_binding] + bindings, set_iam_policy, monkeypatch)
    grant_download_access(EMAIL, "10021", "Participants Info")


def test_revoke_download_access(monkeypatch):
    bindings = [
        _build_binding_with_expiry(
            GOOGLE_DATA_BUCKET, "10021/wes", GOOGLE_DOWNLOAD_ROLE, EMAIL
        ),
        _build_binding_with_expiry(
            GOOGLE_DATA_BUCKET, "10021/cytof", GOOGLE_DOWNLOAD_ROLE, EMAIL
        ),
        {"role": "some-other-role", "members": {f"user:JohnDoe"}},
    ]

    def set_iam_policy(policy):
        print(policy.bindings)
        assert len(policy.bindings) == 2
        assert not any(
            "10021/wes" in binding["condition"]["expression"]
            for binding in policy.bindings
            if "condition" in binding
        )

    # revocation on well-formed bindings
    _mock_gcloud_storage(list(bindings), set_iam_policy, monkeypatch)
    revoke_download_access(EMAIL, "10021", "wes")

    # revocation when target binding doesn't exist
    _mock_gcloud_storage(bindings[1:], set_iam_policy, monkeypatch)
    revoke_download_access(EMAIL, "10021", "wes")

    # revocation when target binding is duplicated
    bindings = [
        _build_binding_with_expiry(
            GOOGLE_DATA_BUCKET, "10021/wes", GOOGLE_DOWNLOAD_ROLE, EMAIL
        ),
        _build_binding_with_expiry(
            GOOGLE_DATA_BUCKET, "10021/wes", GOOGLE_DOWNLOAD_ROLE, EMAIL
        ),
        _build_binding_with_expiry(
            GOOGLE_DATA_BUCKET, "10021/cytof", GOOGLE_DOWNLOAD_ROLE, EMAIL
        ),
        {"role": "some-other-role", "members": {f"user:JohnDoe"}},
    ]
    _mock_gcloud_storage(bindings, set_iam_policy, monkeypatch)
    revoke_download_access(EMAIL, "10021", "wes")


def test_revoke_all_download_access(monkeypatch):
    bindings = [
        {"members": {f"user:{EMAIL}"}, "role": "some-other-role"},
        # This isn't realistic - more likely, there'd be different conditions
        # associated with these duplicate bindings
        {"members": {f"user:{EMAIL}"}, "role": GOOGLE_DOWNLOAD_ROLE},
        {"members": {f"user:{EMAIL}"}, "role": GOOGLE_DOWNLOAD_ROLE},
        {"members": {f"user:{EMAIL}"}, "role": GOOGLE_DOWNLOAD_ROLE},
        {"members": {f"user:{EMAIL}"}, "role": GOOGLE_DOWNLOAD_ROLE},
    ]

    def set_iam_policy(policy):
        assert len(policy.bindings) == 1
        assert policy.bindings[0]["role"] != GOOGLE_DOWNLOAD_ROLE

    # Deletion with many items
    _mock_gcloud_storage(bindings, set_iam_policy, monkeypatch)
    revoke_all_download_access(EMAIL)

    # Idempotent deletion
    _mock_gcloud_storage(bindings[:1], set_iam_policy, monkeypatch)
    revoke_all_download_access(EMAIL)


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
