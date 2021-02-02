import json
from io import BytesIO
from unittest.mock import MagicMock, call
from datetime import datetime

from werkzeug.datastructures import FileStorage
from google.api_core.iam import Policy

from cidc_api.shared import gcloud_client
from cidc_api.config import settings
from cidc_api.models.models import Users
from cidc_api.shared.gcloud_client import (
    create_intake_bucket,
    grant_upload_access,
    refresh_intake_access,
    revoke_upload_access,
    grant_download_access,
    revoke_download_access,
    revoke_all_download_access,
    _xlsx_gcs_uri_format,
    upload_xlsx_to_gcs,
    _pseudo_blob,
    _build_binding_with_expiry,
    upload_xlsx_to_intake_bucket,
)
from cidc_api.config.settings import (
    GOOGLE_INTAKE_ROLE,
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

    create_intake_bucket(EMAIL)

    # Bucket name should have structure:
    # <intake bucket prefix>-<10 character email hash>
    name, hash = storage_client.bucket.call_args[0][0].rsplit("-", 1)
    assert name == GOOGLE_INTAKE_BUCKET
    assert len(hash) == 10 and EMAIL not in hash

    # The bucket gets created and permissions get granted
    storage_client.create_bucket.assert_called_once_with(bucket)
    bucket.get_iam_policy.assert_called_once()
    bucket.set_iam_policy.assert_called_once_with(policy)
    assert len(policy.bindings) == 1
    assert policy.bindings[0]["role"] == GOOGLE_INTAKE_ROLE
    assert policy.bindings[0]["members"] == {f"user:{EMAIL}"}

    # If the bucket already exists, it doesn't get re-created
    storage_client.create_bucket.reset_mock()
    bucket.exists.return_value = True
    create_intake_bucket(EMAIL)
    storage_client.create_bucket.assert_not_called()


def test_refresh_intake_access(monkeypatch):
    _mock_gcloud_storage(
        [
            _build_binding_with_expiry(
                GOOGLE_INTAKE_BUCKET, None, GOOGLE_INTAKE_ROLE, EMAIL
            )
        ],
        lambda i: i,
        monkeypatch,
    )

    grant_expiring_gcs_access = MagicMock()
    monkeypatch.setattr(
        "cidc_api.shared.gcloud_client.grant_expiring_gcs_access",
        grant_expiring_gcs_access,
    )

    refresh_intake_access(EMAIL)
    args = grant_expiring_gcs_access.call_args[0]
    assert args[0].name.startswith(GOOGLE_INTAKE_BUCKET)
    assert args[1:] == (GOOGLE_INTAKE_ROLE, EMAIL)


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
        # should have structure "<expiry time> && <prefix condition>""
        assert len(condition["expression"].split(" && ")) == 2

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
