"""Utilities for interacting with the Google Cloud Platform APIs."""

import datetime

from google.cloud import storage
from google.cloud import pubsub

from config.settings import (
    GOOGLE_UPLOAD_ROLE,
    GOOGLE_UPLOAD_BUCKET,
    GOOGLE_UPLOAD_TOPIC,
    GOOGLE_CLOUD_PROJECT,
)


def _get_bucket(bucket_name: str) -> storage.Bucket:
    """Get the bucket with name `bucket_name` from GCS."""
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    return bucket


def _iam_id(user_email: str) -> str:
    """Append the appropriate IAM account type to a user's email"""
    return f"user:{user_email}"


def grant_upload_access(bucket_name: str, user_email: str):
    """
    Grant a user upload access to the given bucket. Upload access
    means a user can write objects to the bucket but cannot delete,
    overwrite, or read objects from this bucket.
    """
    bucket = _get_bucket(bucket_name)

    # Update the bucket IAM policy to include the user as an uploader.
    policy = bucket.get_iam_policy()
    policy[GOOGLE_UPLOAD_ROLE].add(_iam_id(user_email))
    bucket.set_iam_policy(policy)


def revoke_upload_access(bucket_name: str, user_email: str):
    """
    Revoke a user's upload access for the given bucket.
    """
    bucket = _get_bucket(bucket_name)

    # Update the bucket IAM policy to remove the user's uploader privileges.
    policy = bucket.get_iam_policy()
    policy[GOOGLE_UPLOAD_ROLE].discard(_iam_id(user_email))
    bucket.set_iam_policy(policy)


def get_signed_url(object_name: str, method: str = "PUT", expiry_mins: int = 5) -> str:
    """
    Generate a signed URL for `object_name` to give a client temporary access.

    See: https://cloud.google.com/storage/docs/access-control/signing-urls-with-helpers
    """
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(GOOGLE_UPLOAD_BUCKET)
    blob = bucket.blob(object_name)

    # Generate the signed URL, allowing a client to use `method` for `expiry_mins` minutes
    expiration = datetime.timedelta(minutes=expiry_mins)
    url = blob.generate_signed_url(version="v4", expiration=expiration, method=method)

    return url


def publish_upload_success(job_id: int):
    """Publish to the uploads topic that the upload job with the provided `job_id` succeeded."""
    publisher = pubsub.PublisherClient()
    topic = publisher.topic_path(GOOGLE_CLOUD_PROJECT, GOOGLE_UPLOAD_TOPIC)

    # The Pub/Sub publisher client returns a concurrent.futures.Future
    # containing info about whether the publishing was successful.
    data = bytes(str(job_id), "utf-8")
    report = publisher.publish(topic, data=data)

    # For now, just wait till we get a response back from Pub/Sub.
    # If there was an error, the below call will throw an exception.
    # TODO: evaluate if it's worth trying to leverage asynchrony here.
    report.result()
