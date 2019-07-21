"""Utilities for managing access to GCS objects."""

from google.cloud import storage
from settings import GOOGLE_UPLOAD_ROLE


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
