"""Utilities for interacting with the Google Cloud Platform APIs."""
import json
import os

os.environ["TZ"] = "UTC"
import datetime
import warnings
import hashlib
from collections import namedtuple
from concurrent.futures import Future
from typing import BinaryIO, List, Optional, Tuple

import requests
from google.cloud import storage, pubsub
from werkzeug.datastructures import FileStorage

from ..config.settings import (
    GOOGLE_AND_OPERATOR,
    GOOGLE_DOWNLOAD_ROLE,
    GOOGLE_INTAKE_ROLE,
    GOOGLE_INTAKE_BUCKET,
    GOOGLE_UPLOAD_ROLE,
    GOOGLE_UPLOAD_BUCKET,
    GOOGLE_UPLOAD_TOPIC,
    GOOGLE_DATA_BUCKET,
    GOOGLE_LISTER_ROLE,
    GOOGLE_CLOUD_PROJECT,
    GOOGLE_EMAILS_TOPIC,
    GOOGLE_PATIENT_SAMPLE_TOPIC,
    GOOGLE_ARTIFACT_UPLOAD_TOPIC,
    GOOGLE_MAX_CONDITIONAL_OPERATORS,
    GOOGLE_MAX_DOWNLOAD_CONDITIONS,
    GOOGLE_MAX_DOWNLOAD_PERMISSIONS,
    GOOGLE_OR_OPERATOR,
    TESTING,
    ENV,
    DEV_CFUNCTIONS_SERVER,
    INACTIVE_USER_DAYS,
)
from ..config.logging import get_logger

logger = get_logger(__name__)

_storage_client = None


def _get_storage_client() -> storage.Client:
    global _storage_client
    if _storage_client is None:
        _storage_client = storage.Client()
    return _storage_client


def _get_bucket(bucket_name: str) -> storage.Bucket:
    """Get the bucket with name `bucket_name` from GCS."""
    storage_client = _get_storage_client()
    bucket = storage_client.bucket(bucket_name)
    return bucket


_xlsx_gcs_uri_format = (
    "{trial_id}/xlsx/{template_category}/{template_type}/{upload_moment}.xlsx"
)


_pseudo_blob = namedtuple(
    "_pseudo_blob", ["name", "size", "md5_hash", "crc32c", "time_created"]
)


def upload_xlsx_to_gcs(
    trial_id: str,
    template_category: str,
    template_type: str,
    filebytes: BinaryIO,
    upload_moment: str,
):
    """
    Upload an xlsx template file to GCS, returning the object URI.
    
    `template_category` is either "manifests" or "assays".
    `template_type` is an assay or manifest type, like "wes" or "pbmc" respectively.

    Returns:
        arg1: GCS blob object
    """
    blob_name = _xlsx_gcs_uri_format.format(
        trial_id=trial_id,
        template_category=template_category,
        template_type=template_type,
        upload_moment=upload_moment,
    )

    if ENV == "dev":
        logger.info(
            f"Would've saved {blob_name} to {GOOGLE_UPLOAD_BUCKET} and {GOOGLE_DATA_BUCKET}"
        )
        return _pseudo_blob(
            blob_name, 0, "_pseudo_md5_hash", "_pseudo_crc32c", upload_moment
        )

    upload_bucket: storage.Bucket = _get_bucket(GOOGLE_UPLOAD_BUCKET)
    blob = upload_bucket.blob(blob_name)

    filebytes.seek(0)
    blob.upload_from_file(filebytes)

    data_bucket = _get_bucket(GOOGLE_DATA_BUCKET)
    final_object = upload_bucket.copy_blob(blob, data_bucket)

    return final_object


def grant_lister_access(user_email: str):
    """
    Grant a user list access to the GOOGLE_DATA_BUCKET. List access is
    required for the user to download or read objects from this bucket.
    """
    logger.info(f"granting list to {user_email}")
    bucket = _get_bucket(GOOGLE_DATA_BUCKET)
    grant_gcs_access(bucket, GOOGLE_LISTER_ROLE, user_email, expiring=True)


def revoke_lister_access(user_email: str):
    """
    Revoke a user's list access to the GOOGLE_DATA_BUCKET. List access is
    required for the user to download or read objects from this bucket.
    """
    logger.info(f"revoking list to {user_email}")
    bucket = _get_bucket(GOOGLE_DATA_BUCKET)
    revoke_expiring_gcs_access(bucket, GOOGLE_LISTER_ROLE, user_email)


def grant_upload_access(user_email: str):
    """
    Grant a user upload access to the GOOGLE_UPLOAD_BUCKET. Upload access
    means a user can write objects to the bucket but cannot delete,
    overwrite, or read objects from this bucket.
    """
    logger.info(f"granting upload to {user_email}")
    bucket = _get_bucket(GOOGLE_UPLOAD_BUCKET)

    # Update the bucket IAM policy to include the user as an uploader.
    policy = bucket.get_iam_policy()
    policy[GOOGLE_UPLOAD_ROLE] = {*policy[GOOGLE_UPLOAD_ROLE], f"user:{user_email}"}
    logger.info(f"{GOOGLE_UPLOAD_ROLE} binding updated to {policy[GOOGLE_UPLOAD_ROLE]}")

    try:
        bucket.set_iam_policy(policy)
    except Exception as e:
        logger.error(str(e))
        raise e


def revoke_upload_access(user_email: str):
    """
    Revoke a user's upload access for the given bucket.
    """
    logger.info(f"revoking upload from {user_email}")
    bucket = _get_bucket(GOOGLE_UPLOAD_BUCKET)

    # Update the bucket IAM policy to remove the user's uploader privileges.
    policy = bucket.get_iam_policy()
    policy[GOOGLE_UPLOAD_ROLE].discard(f"user:{user_email}")
    logger.info(f"{GOOGLE_UPLOAD_ROLE} binding updated to {policy[GOOGLE_UPLOAD_ROLE]}")

    try:
        bucket.set_iam_policy(policy)
    except Exception as e:
        logger.error(str(e))
        raise e


def get_intake_bucket_name(user_email: str) -> str:
    """
    Get the name for an intake bucket associated with the given user.
    Bucket names will have a structure like <intake
    """
    # 10 characters should be plenty, given that we only expect
    # a handful of unique data uploaders - we get 16^10 possible hashes.
    email_hash = hashlib.sha1(bytes(user_email, "utf-8")).hexdigest()[:10]
    bucket_name = f"{GOOGLE_INTAKE_BUCKET}-{email_hash}"
    return bucket_name


def create_intake_bucket(user_email: str) -> storage.Bucket:
    """
    Create a new data intake bucket for this user, or get the existing one.
    Grant the user GCS object admin permissions on the bucket, or refresh those
    permissions if they've already been granted.
    """
    storage_client = _get_storage_client()
    bucket_name = get_intake_bucket_name(user_email)
    bucket = storage_client.bucket(bucket_name)

    if not bucket.exists():
        # Create a new bucket with bucket-level permissions enabled.
        bucket.iam_configuration.uniform_bucket_level_access_enabled = True
        bucket = storage_client.create_bucket(bucket)

    # Grant the user appropriate permissions
    grant_gcs_access(bucket, GOOGLE_INTAKE_ROLE, user_email, expiring=True)

    return bucket


def refresh_intake_access(user_email: str):
    """
    Re-grant a user's access to their intake bucket if it exists.
    """
    bucket_name = get_intake_bucket_name(user_email)
    bucket = _get_bucket(bucket_name)

    if bucket.exists():
        grant_gcs_access(bucket, GOOGLE_INTAKE_ROLE, user_email, expiring=True)


def upload_xlsx_to_intake_bucket(
    user_email: str, trial_id: str, upload_type: str, xlsx: FileStorage
) -> str:
    """
    Upload a metadata spreadsheet file to the GCS intake bucket, 
    returning the URL to the bucket in the GCP console.
    """
    # add a timestamp to the metadata file name to avoid overwriting previous versions
    filename_with_ts = f'{xlsx.filename.rsplit(".xlsx", 1)[0]}_{datetime.datetime.now().isoformat()}.xlsx'
    blob_name = f"{trial_id}/{upload_type}/metadata/{filename_with_ts}"

    # upload the metadata spreadsheet to the intake bucket
    bucket_name = get_intake_bucket_name(user_email)
    bucket = _get_bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_file(xlsx)

    return f"https://console.cloud.google.com/storage/browser/_details/{bucket_name}/{blob_name}"


def grant_download_access(
    user_email: str, trial_id: Optional[str], upload_type: Optional[str]
):
    """
    Give a user download access to all objects in a trial of a particular upload type.

    If trial_id is None, then grant access to all trials.

    If upload_type is None, then grant access to all upload_types.

    If the user already has download access for this trial and upload type, idempotent.
    """
    prefixes = _build_trial_upload_prefixes(trial_id, upload_type)

    logger.info(f"Granting download access on prefixes {prefixes} to {user_email}")

    bucket = _get_bucket(GOOGLE_DATA_BUCKET)
    grant_gcs_access(
        bucket, GOOGLE_DOWNLOAD_ROLE, user_email, expiring=False, prefixes=prefixes
    )


def revoke_download_access(
    user_email: str, trial_id: Optional[str], upload_type: Optional[str]
):
    """
    Revoke a user's download access to all objects in a trial of a particular upload type.

    Return the GCS URI from which access has been revoked.
    """
    prefixes = _build_trial_upload_prefixes(trial_id, upload_type)

    logger.info(f"Revoking download access on {prefixes} from {user_email}")

    bucket = _get_bucket(GOOGLE_DATA_BUCKET)
    return revoke_nonexpiring_gcs_access(
        bucket, GOOGLE_DOWNLOAD_ROLE, user_email, prefixes
    )


def _build_trial_upload_prefixes(
    trial_id: Optional[str], upload_type: Optional[str]
) -> List[str]:
    """
    Build the set of prefixes associated with the trial_id and upload_type
    If no trial_id is given, all trials are used.
    If no upload_type is given, the prefix is only defined to the trial.
    If neither are given, an empty string is returned.
    """
    if trial_id is None and upload_type is None:
        return [""]

    if not trial_id:
        from ..models.models import TrialMetadata

        trial_id = set([t.trial_id for t in TrialMetadata.list()])

    if not upload_type:
        return list(trial_id) if isinstance(trial_id, set) else [trial_id]
    else:
        broad_upload_type = upload_type.lower().replace(" ", "_").split("_", 1)[0]
        return [
            f"{trial}/{broad_upload_type}"
            for trial in (trial_id if isinstance(trial_id, set) else [trial_id])
        ]


def grant_gcs_access(
    bucket: storage.Bucket,
    role: str,
    user_email: str,
    expiring: bool = True,
    prefixes: List[str] = [""],
):
    """
    Grant `user_email` the provided `role` on a `bucket`, possibly expiring after `INACTIVE_USER_DAYS` 
    days have elapsed. By default, permissions apply to the whole bucket. Optionally, provide 
    an object URL `prefix` to restrict this permission grant to only a portion of the objects 
    in the given bucket.
    """
    # see https://cloud.google.com/storage/docs/access-control/using-iam-permissions#code-samples_3
    policy = bucket.get_iam_policy(requested_policy_version=3)
    policy.version = 3

    # remove the existing binding if one exists so that we can recreate it with
    # an updated TTL.
    all_other_conditions = []
    for prefix in prefixes:
        _, other_conditions = _find_and_pop_binding(
            policy, role, user_email, prefix=prefix, return_next=True
        )
        all_other_conditions.extend(other_conditions)

    # will return a set of additional conditions to extend if available
    if expiring:
        bindings = _build_bindings_with_expiry(
            bucket.name,
            role,
            user_email,
            prefixes=prefixes,
            other_conditions=all_other_conditions,
        )
    else:
        bindings = _build_bindings_without_expiry(
            bucket.name,
            role,
            user_email,
            prefixes=prefixes,
            other_conditions=all_other_conditions,
        )

    # (re)insert the binding into the policy
    policy.bindings.extend(bindings)

    try:
        bucket.set_iam_policy(policy)
    except Exception as e:
        logger.error(str(e))
        raise e


def revoke_nonexpiring_gcs_access(
    bucket: storage.Bucket, role: str, user_email: str, prefixes: List[str] = [""]
):
    """Revoke a bucket IAM policy change made by calling `grant_gcs_access` with expiring=False."""
    # see https://cloud.google.com/storage/docs/access-control/using-iam-permissions#code-samples_3
    policy = bucket.get_iam_policy(requested_policy_version=3)
    policy.version = 3

    # find and remove all matching policy bindings for this user if any exist
    for prefix in prefixes:
        for i in range(GOOGLE_MAX_DOWNLOAD_PERMISSIONS):
            removed_binding, other_conditions = _find_and_pop_binding(
                policy, role, user_email, prefix=prefix, return_next=False
            )

            if removed_binding is None:
                if i == 0:
                    warnings.warn(
                        f"Tried to revoke a non-existent download IAM permission for {user_email}/{prefix}"
                    )
                break

            # with only return others if removed_binding is not None
            elif len(other_conditions):
                readd_bindings = _build_bindings_without_expiry(
                    bucket.name, role, user_email, other_conditions=other_conditions
                )
                policy.bindings.extend(readd_bindings)

    try:
        bucket.set_iam_policy(policy)
    except Exception as e:
        logger.error(str(e))
        raise e


def revoke_expiring_gcs_access(
    bucket: storage.Bucket, role: str, user_email: str, prefixes: List[str] = [""]
):
    """Revoke a bucket IAM policy made by calling `grant_gcs_access` with (default) expiring=True."""
    # see https://cloud.google.com/storage/docs/access-control/using-iam-permissions#code-samples_3
    policy = bucket.get_iam_policy(requested_policy_version=3)
    policy.version = 3

    # find and remove all matching policy bindings for this user if any exist
    all_other_conditions = []
    for prefix in prefixes:
        for i in range(GOOGLE_MAX_DOWNLOAD_CONDITIONS):
            removed_binding, other_conditions = _find_and_pop_binding(
                policy, role, user_email, prefix=prefix, return_next=False
            )
            if removed_binding is None:
                if i == 0:
                    warnings.warn(
                        f"Tried to revoke a non-existent download IAM permission for {user_email}/{prefix}"
                    )
                break

            # with only return others if removed_binding is not None
            else:
                all_other_conditions.extend(other_conditions)

    readd_bindings = _build_bindings_without_expiry(
        bucket.name,
        role,
        user_email,
        prefixes=[],  # explicitly not providing any new bindings here
        other_conditions=all_other_conditions,
    )
    policy.bindings.extend(readd_bindings)
    try:
        bucket.set_iam_policy(policy)
    except Exception as e:
        logger.error(str(e))
        raise e


# Arbitrary upper bound on the number of GCS bindings we expect a user to have
MAX_REVOKE_ALL_ITERATIONS = 250


def revoke_all_download_access(user_email: str):
    """
    Completely revoke a user's download access to all objects in the data bucket.
    """
    # get the current IAM policy for the data bucket
    bucket = _get_bucket(GOOGLE_DATA_BUCKET)
    # see https://cloud.google.com/storage/docs/access-control/using-iam-permissions#code-samples_3
    policy = bucket.get_iam_policy(requested_policy_version=3)
    policy.version = 3

    # find and pop all download role policy bindings for this user
    for _ in range(MAX_REVOKE_ALL_ITERATIONS):
        # this finds and removes *any* download binding for the given user_email
        # since we're popping them all, we don't care to readd any other conditions
        if _find_and_pop_binding(policy, GOOGLE_DOWNLOAD_ROLE, user_email)[0] is None:
            break

    try:
        bucket.set_iam_policy(policy)
    except Exception as e:
        logger.error(str(e))
        raise e


user_member = lambda email: f"user:{email}"


def _build_bindings_without_expiry(
    bucket: str,
    role: str,
    user_email: str,
    prefixes: List[str] = [""],
    other_conditions: List[str] = [],
) -> List[dict]:
    """
    Grant the user associated with `user_email` the provided IAM `role` when acting on objects
    in `bucket` whose URIs start with any value in `prefixes`. These permissions remains active
    indefinitely, as the required Lister role is applied with an expiry.

    GCP IAM only allows up to 12 conditional operators ie combine 13 conditions
    Therefore must split into multiple permissions of 13 conditions each.
    As you can only have 20 permissions total ie 19 plus Lister, errors out beyond that.

    See GCP common expression language syntax overview: https://cloud.google.com/iam/docs/conditions-overview

    Parameters
    ----------
    bucket: str
        the name of the bucket to build the binding for
    role: str
        the role name to build the binding for
    user_email: str
        the email of the user to build the binding for
    prefixes: List[str] = [""]
        a list of prefixes used to build resource.name.startsWith conditions
        can't have more than GOOGLE_MAX_DOWNLOAD_CONDITIONS entries
    other_conditions: List[str] = []
        any already formatted conditions, such as returned from _find_and_pop_binding

    Returns
    -------
    List[dict]
        the bindings to be put onto policy.bindings
    """
    if len(prefixes) > GOOGLE_MAX_DOWNLOAD_CONDITIONS:
        raise Exception(
            f"A single user cannot have more than {GOOGLE_MAX_DOWNLOAD_CONDITIONS} download conditions"
        )

    timestamp = datetime.datetime.now()
    return [
        {
            "role": role,
            "members": {user_member(user_email)},  # convert format
            "condition": {
                "title": f"{role} access on {prefixes or 'bucket'}",
                "description": f"Auto-updated by the CIDC API on {timestamp}",
                # since this is non-expiring, all operators are OR and no brackets are needed
                "expression": GOOGLE_OR_OPERATOR.join(
                    [
                        # put the other conditions in directly
                        other_conditions.pop(0)
                        # since we're using pop, have to stop at the end
                        if len(other_conditions)
                        # format object URL prefixes to the condition if specified
                        else f'resource.name.startsWith("projects/_/buckets/{bucket}/objects/{prefixes.pop(0)}")'
                        # can only have a certain number of operators, plus one for entries
                        for _ in range(GOOGLE_MAX_CONDITIONAL_OPERATORS + 1)
                        # since we're using pop, have to stop at the end
                        if len(other_conditions) or (prefixes and len(prefixes))
                    ]
                ),
            },
        }
        # they can only have a certain number of permissions
        for _ in range(GOOGLE_MAX_DOWNLOAD_PERMISSIONS)
        # since we're using pop above, have to stop at the end
        if len(other_conditions) or (prefixes and len(prefixes))
    ]


def _build_bindings_with_expiry(
    bucket: str,
    role: str,
    user_email: str,
    ttl_days: int = INACTIVE_USER_DAYS,
    prefixes: List[str] = [""],
    other_conditions: List[str] = [],
) -> dict:
    """
    Grant the user associated with `user_email` the provided IAM `role` when acting
    on objects in `bucket` whose URIs start with `prefix`. This permission remains active
    for `ttl_days` days.

    See GCP common expression language syntax overview: https://cloud.google.com/iam/docs/conditions-overview
    
    Parameters
    ----------
    bucket: str
        the name of the bucket to build the binding for
    role: str
        the role name to build the binding for
    user_email: str
        the email of the user to build the binding for
    ttl_days: int = INACTIVE_USER_DAYS
        the number of days until this permission should expire
    prefixes: List[str] = [""]
        a list of prefixes used to build resource.name.startsWith conditions
        can't have more than GOOGLE_MAX_DOWNLOAD_CONDITIONS entries
    other_conditions: List[str] = []
        any already formatted conditions, such as returned from _find_and_pop_binding

    Returns
    -------
    List[dict]
        the bindings to be put onto policy.bindings
    """
    timestamp = datetime.datetime.now()
    expiry_date = (timestamp + datetime.timedelta(ttl_days)).date()

    # use all of the prefixes for the name of the bindings
    all_prefixes = prefixes.copy() if prefixes else []
    all_prefixes.extend(
        [c.split("/objects/")[-1].strip('")') for c in other_conditions]
    )

    # going to add the expiration after, so don't return directly
    ret = [
        {
            "role": role,
            "members": {user_member(user_email)},  # convert format
            "condition": {
                "title": f"{role} access on {all_prefixes}",
                "description": f"Auto-updated by the CIDC API on {timestamp}",
                # since this is expiring, all operators are OR
                # if there are no entries here, we don't need brackets; so we'll deal with it later
                "expression": GOOGLE_OR_OPERATOR.join(
                    [
                        # put the other conditions in directly
                        other_conditions.pop(0)
                        # since we're using pop, have to stop at the end
                        if len(other_conditions)
                        # format object URL prefixes to the condition if specified
                        else f'resource.name.startsWith("projects/_/buckets/{bucket}/objects/{prefixes.pop(0)}")'
                        # can only have a certain number of operators, plus one for entries but minus one for the AND
                        # the -1 here is why this differs from the _build_bindings_without_expiry
                        for _ in range(GOOGLE_MAX_CONDITIONAL_OPERATORS)
                        # since we're using pop, have to stop at the end
                        if len(other_conditions) or (prefixes and len(prefixes))
                    ]
                ),
            },
        }
        # they can only have a certain number of permissions
        for _ in range(GOOGLE_MAX_DOWNLOAD_PERMISSIONS)
        # since we're using pop, have to stop at the end
        if len(other_conditions) or (prefixes and len(prefixes))
    ]

    # Add the TTL clause
    new_condition = f'request.time < timestamp("{expiry_date.isoformat()}T00:00:00Z")'
    for i in range(len(ret)):  # for each binding
        if len(ret[i]["condition"]["expression"]):
            # if it has OR conditions, it need brackets
            ret[i]["condition"]["expression"] = (
                "(" + ret[i]["condition"]["expression"] + ")" + GOOGLE_AND_OPERATOR
            )
        # add the expiration
        ret[i]["condition"]["expression"] += new_condition

    return ret


def _can_add_more_conditions(binding: dict) -> bool:
    """Return if there is more space to add further prefix conditions, accounting for expiry"""
    expression = binding.get("condition", {}).get("expression", "")
    return (
        expression.count(GOOGLE_OR_OPERATOR) + expression.count(GOOGLE_AND_OPERATOR)
        < GOOGLE_MAX_CONDITIONAL_OPERATORS
    )


def _find_and_pop_binding(
    policy: storage.bucket.Policy,
    role: str,
    user_email: str,
    prefix: str = "",
    return_next: bool = False,
) -> Tuple[Optional[dict], List[str]]:
    """
    Find an IAM policy binding for the given `user_email`, `policy`, and `role`, and pop
    it from the policy's bindings list if it exists. Matches the `prefix` if given.

    Also returns the rest of the conditions if they exist to be readded to the bindings later.
    If no matching binding is found and `return_next`, the last set of conditions for that
        role/user_email if that permission can be extended.
    In all other cases, returns an empty list.
    """
    # try to find the policy binding on the `policy`
    user_binding_index = None
    extendable_index = None
    for i, binding in enumerate(policy.bindings):
        role_matches = binding.get("role") == role
        member_matches = binding.get("members") == {user_member(user_email)}
        prefix_matches = prefix in binding.get("condition", {}).get("expression", "")
        if role_matches and member_matches and prefix_matches:
            # a user should be a member of no more than one conditional download binding
            # if they do, warn - but use the last one because this isn't breaking
            if (
                user_binding_index is not None
                or binding.get("condition", {}).get("expression", "").count(prefix) > 1
            ):
                warnings.warn(
                    f"Found multiple conditional bindings for {user_email} on {prefix} role {role}. This is an invariant violation - "
                    "check out permissions on the CIDC GCS buckets to debug."
                )
            user_binding_index = i

        # return the last policy binding if it doesn't have
        elif (
            return_next
            and role_matches
            and member_matches
            and _can_add_more_conditions(binding)
        ):
            if extendable_index is not None:
                warnings.warn(
                    f"Found multiple extendable bindings for {user_email} role {role}"
                )
            extendable_index = i

    binding = (
        policy.bindings.pop(user_binding_index)
        if user_binding_index is not None
        else (
            policy.bindings.pop(extendable_index)
            if extendable_index is not None
            else None
        )
    )

    # if it's an expiring permission, it'll be in the form: (prefix or prefix2) and time
    # # old permissions are in the form: time and prefix
    prefix_conditions = (
        binding.get("condition", {}).get("expression", "") if binding else ""
    )
    if GOOGLE_AND_OPERATOR in prefix_conditions:
        # clean up parentheses
        prefix_conditions = prefix_conditions.split(GOOGLE_AND_OPERATOR)
        if "resource.name.startsWith" in prefix_conditions[1]:
            # old-style: time and prefix
            prefix_conditions = prefix_conditions[1]
        else:
            # (prefix or prefix2) and time
            prefix_conditions = prefix_conditions[0].strip("()")

    remaining_conditions = [
        condition
        for condition in prefix_conditions.split(GOOGLE_OR_OPERATOR)
        if prefix and prefix not in condition and len(condition)
    ]

    return binding, remaining_conditions


def get_signed_url(
    object_name: str,
    bucket_name: str = GOOGLE_DATA_BUCKET,
    method: str = "GET",
    expiry_mins: int = 30,
) -> str:
    """
    Generate a signed URL for `object_name` to give a client temporary access.

    Using v2 signed urls because v4 is in Beta and response_disposition doesn't work.
    https://cloud.google.com/storage/docs/access-control/signing-urls-with-helpers
    """
    storage_client = _get_storage_client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(object_name)

    # Generate the signed URL, allowing a client to use `method` for `expiry_mins` minutes
    expiration = datetime.timedelta(minutes=expiry_mins)
    full_filename = object_name.replace("/", "_").replace('"', "_").replace(" ", "_")
    url = blob.generate_signed_url(
        version="v2",
        expiration=expiration,
        method=method,
        response_disposition=f'attachment; filename="{full_filename}"',
    )
    logger.info(f"generated signed URL for {object_name}: {url}")

    return url


def _encode_and_publish(content: str, topic: str) -> Future:
    """Convert `content` to bytes and publish it to `topic`."""
    pubsub_publisher = pubsub.PublisherClient()
    topic = pubsub_publisher.topic_path(GOOGLE_CLOUD_PROJECT, topic)
    data = bytes(content, "utf-8")

    # Don't actually publish to Pub/Sub if running locally
    if ENV == "dev":
        if DEV_CFUNCTIONS_SERVER:
            logger.info(
                f"Publishing message {content!r} to topic {DEV_CFUNCTIONS_SERVER}/{topic}"
            )
            import base64

            bdata = base64.b64encode(content.encode("utf-8"))
            try:
                res = requests.post(
                    f"{DEV_CFUNCTIONS_SERVER}/{topic}", data={"data": bdata}
                )
            except Exception as e:
                raise Exception(
                    f"Couldn't publish message {content!r} to topic {DEV_CFUNCTIONS_SERVER}/{topic}"
                ) from e
            else:
                logger.info(f"Got {res}")
                if res.status_code != 200:
                    raise Exception(
                        f"Couldn't publish message {content!r} to {DEV_CFUNCTIONS_SERVER}/{topic}: {res!r}"
                    )
        else:
            logger.info(f"Would've published message {content} to topic {topic}")
        return

    # The Pub/Sub publisher client returns a concurrent.futures.Future
    # containing info about whether the publishing was successful.
    report = pubsub_publisher.publish(topic, data=data)

    return report


def publish_upload_success(job_id: int):
    """Publish to the uploads topic that the upload job with the provided `job_id` succeeded."""
    report = _encode_and_publish(str(job_id), GOOGLE_UPLOAD_TOPIC)

    # For now, we wait await this Future. Going forward, maybe
    # we should look for a way to leverage asynchrony here.
    if report:
        report.result()


def publish_patient_sample_update(manifest_upload_id: int):
    """Publish to the patient_sample_update topic that a new manifest has been uploaded."""
    report = _encode_and_publish(str(manifest_upload_id), GOOGLE_PATIENT_SAMPLE_TOPIC)

    # Wait for response from pub/sub
    if report:
        report.result()


def publish_artifact_upload(file_id: int):
    """Publish a downloadable file ID to the artifact_upload topic"""
    report = _encode_and_publish(str(file_id), GOOGLE_ARTIFACT_UPLOAD_TOPIC)

    # Wait for response from pub/sub
    if report:
        report.result()


def send_email(to_emails: List[str], subject: str, html_content: str, **kw):
    """
    Publish an email-to-send to the emails topic.
    `kw` are expected to be sendgrid json api style additional email parameters. 
    """
    # Don't actually send an email if this is a test
    if TESTING or ENV == "dev":
        logger.info(f"Would send email with subject '{subject}' to {to_emails}")
        return

    email_json = json.dumps(
        dict(to_emails=to_emails, subject=subject, html_content=html_content, **kw)
    )

    report = _encode_and_publish(email_json, GOOGLE_EMAILS_TOPIC)

    # Await confirmation that the published message was received.
    if report:
        report.result()
