import os
import shutil
import tempfile
from uuid import uuid4
from io import BytesIO
from concurrent.futures import ThreadPoolExecutor
from typing import List

from flask import Blueprint, jsonify, send_file
from webargs import fields
from webargs.flaskparser import use_args
from werkzeug.exceptions import BadRequest, NotFound, Unauthorized


from ..models import (
    DownloadableFiles,
    DownloadableFileSchema,
    DownloadableFileListSchema,
    Permissions,
    ROLES,
    CIDCRole,
)
from ..shared import gcloud_client
from ..shared.auth import get_current_user, requires_auth
from ..shared.rest_utils import with_lookup, marshal_response, use_args_with_pagination
from ..config.settings import (
    ENV,
    GOOGLE_ACL_DATA_BUCKET,
    GOOGLE_DATA_BUCKET,
    GOOGLE_EPHEMERAL_BUCKET,
    MAX_THREADPOOL_WORKERS,
)


downloadable_files_bp = Blueprint("downloadable_files", __name__)

downloadable_files_schema = DownloadableFileSchema()
downloadable_files_list_schema = DownloadableFileListSchema()


file_filter_schema = {
    "trial_ids": fields.DelimitedList(fields.Str),
    "facets": fields.DelimitedList(fields.DelimitedList(fields.Str, delimiter="|")),
}


@downloadable_files_bp.route("/", methods=["GET"])
@requires_auth("downloadable_files")
@use_args_with_pagination(file_filter_schema, downloadable_files_schema)
@marshal_response(downloadable_files_list_schema)
def list_downloadable_files(args, pagination_args):
    """List downloadable files that the current user is allowed to view."""
    user = get_current_user()

    filter_ = DownloadableFiles.build_file_filter(**args, user=user)

    files = DownloadableFiles.list(filter_=filter_, **pagination_args)
    count = DownloadableFiles.count(filter_=filter_)

    return {"_items": files, "_meta": {"total": count}}


@downloadable_files_bp.route("/<int:downloadable_file>", methods=["GET"])
@requires_auth("downloadable_files")
@with_lookup(DownloadableFiles, "downloadable_file")
@marshal_response(downloadable_files_schema)
def get_downloadable_file(downloadable_file: DownloadableFiles) -> DownloadableFiles:
    """Get a single file by ID if the current user is allowed to view it."""
    user = get_current_user()

    # Admins can view any file
    if user.is_admin():
        # this is not user-input due to @with_lookup, so safe to return
        return downloadable_file

    # Check that a non-admin has permission to view this file
    perm = Permissions.find_for_user_trial_type(
        user.id, downloadable_file.trial_id, downloadable_file.upload_type
    )

    if not perm:
        raise Unauthorized()

    # this is not user-input due to @with_lookup, so safe to return
    return downloadable_file


@downloadable_files_bp.route("/<int:downloadable_file>/related_files", methods=["GET"])
@requires_auth("downloadable_files")
@with_lookup(DownloadableFiles, "downloadable_file")
@marshal_response(downloadable_files_list_schema)
def get_related_files(downloadable_file: DownloadableFiles):
    """Get files related to the given `downloadable_file`."""
    user = get_current_user()

    if not user.is_admin() and not Permissions.find_for_user_trial_type(
        user.id, downloadable_file.trial_id, downloadable_file.upload_type
    ):
        raise Unauthorized()

    return {"_items": downloadable_file.get_related_files()}


def _get_object_urls_or_404(ids: List[int]) -> List[str]:
    """
    Get the list of object URLs associated with the given IDs. If none are found,
    raise a NotFound exception (HTTP 404).
    """

    current_user = get_current_user()
    user_perms_filter = DownloadableFiles.build_file_filter(user=current_user)
    urls = DownloadableFiles.list_object_urls(ids, filter_=user_perms_filter)

    # If the query returned no files, respond with 404
    if len(urls) == 0:
        raise NotFound()

    return urls


MAX_BUNDLE_BYTES = int(1e8)  # 100MB


@downloadable_files_bp.route("/compressed_batch", methods=["POST"])
@requires_auth("compressed_batch")
@use_args({"file_ids": fields.List(fields.Int, required=True)}, location="json")
def create_compressed_batch(args):
    """
    Given a list of file ids, download those files from GCS and compress them
    into a single file. Respond with a GCS signed URL for downloading the
    compressed file.

    Currently, onl file batches with size <=100MB are supported. If the total file
    size of the requested files is greater than 100MB, respond with HTTP status code
    400 (Bad Request).
    """
    urls = _get_object_urls_or_404(args["file_ids"])

    # Check that total requested file size doesn't exceed the maximum
    file_filter = lambda q: q.filter(DownloadableFiles.object_url.in_(urls))
    if DownloadableFiles.get_total_bytes(filter_=file_filter) > MAX_BUNDLE_BYTES:
        raise BadRequest(
            f"batch too large: can't directly download a batch with more than {MAX_BUNDLE_BYTES} bytes"
        )

    data_bucket = gcloud_client._get_bucket(
        GOOGLE_DATA_BUCKET if ENV == "prod" else GOOGLE_ACL_DATA_BUCKET
    )
    # Using a temporary directory allows us to avoid collisions
    # with other possible concurrent requests to this endpoint
    # and to get automatic cleanup of all data we write once we're
    # done using the directory.
    with tempfile.TemporaryDirectory() as tmpdir:
        indir = os.path.join(tmpdir, "in")
        os.mkdir(indir)
        # Download all files in the batch to a subdirectory in the tmpdir.
        # Since this process is I/O-bound, not CPU-bound, we get a
        # performance benefit from multithreading it.
        with ThreadPoolExecutor(MAX_THREADPOOL_WORKERS) as pool:
            filename = lambda url: os.path.join(indir, url.replace("/", "_"))
            download = lambda url: data_bucket.get_blob(url).download_to_filename(
                filename(url)
            )
            pool.map(download, urls)

        # Create a compressed file from the contents of the temporary directory
        random_filename = str(uuid4())
        outpath = shutil.make_archive(
            os.path.join(tmpdir, random_filename), "gztar", indir
        )

        # Upload the compressed file to the ephemeral bucket, where
        # it will exist for a day before being auto-deleted.
        ephemeral_bucket = gcloud_client._get_bucket(GOOGLE_EPHEMERAL_BUCKET)
        blob = ephemeral_bucket.blob(os.path.basename(outpath))
        blob.upload_from_filename(outpath)

    # Get a signed URL for the download blob
    download_url = gcloud_client.get_signed_url(blob.name, GOOGLE_EPHEMERAL_BUCKET)

    return jsonify(download_url)


@downloadable_files_bp.route("/filelist", methods=["POST"])
@requires_auth("filelist")
@use_args({"file_ids": fields.List(fields.Int, required=True)}, location="json")
def generate_filelist(args):
    """
    Return a file `filelist.tsv` mapping GCS URIs to flat filenames for the
    provided set of file ids.
    """
    urls = _get_object_urls_or_404(args["file_ids"])

    # Build TSV mapping GCS URIs to flat filenames
    # (bytes because that's what send_file knows how to send)
    tsv = b""
    for url in urls:
        flat_url = url.replace("/", "_")
        full_gcs_uri = f"gs://{GOOGLE_DATA_BUCKET if ENV == 'prod' else GOOGLE_ACL_DATA_BUCKET}/{url}"
        tsv += bytes(f"{full_gcs_uri}\t{flat_url}\n", "utf-8")

    buffer = BytesIO(tsv)

    return send_file(
        buffer,
        as_attachment=True,
        attachment_filename="filelist.tsv",
        mimetype="text/tsv",
    )


@downloadable_files_bp.route("/download_url", methods=["GET"])
@requires_auth(
    "download_url",
    allowed_roles=[role for role in ROLES if role != CIDCRole.NETWORK_VIEWER.value],
)
@use_args({"id": fields.Str(required=True)}, location="query")
def get_download_url(args):
    """
    Get a signed GCS download URL for a given file.
    """
    # Extract file ID from route
    file_id = args["id"]

    # Check that file exists
    file_record = DownloadableFiles.find_by_id(file_id)
    if not file_record:
        raise NotFound(f"No file with id {file_id}.")

    user = get_current_user()

    # Ensure user has permission to access this file
    if not user.is_admin():
        perm = Permissions.find_for_user_trial_type(
            user.id, file_record.trial_id, file_record.upload_type
        )
        if not perm:
            raise NotFound(f"No file with id {file_id}.")

    # Generate the signed URL and return it.
    download_url = gcloud_client.get_signed_url(file_record.object_url)
    return jsonify(download_url)


@downloadable_files_bp.route("/filter_facets", methods=["GET"])
@requires_auth("filter_facets")
@use_args(file_filter_schema, location="query")
def get_filter_facets(args):
    """
    Return a list of allowed filter facet values for a user.
    Response will have structure:
    {
        <facet 1>: [<value 1>, <value 2>,...],
        <facet 2>: [...],
        ...
    }
    """
    user = get_current_user()
    trial_ids = DownloadableFiles.get_trial_facets(
        filter_=DownloadableFiles.build_file_filter(
            facets=args.get("facets"), user=user
        )
    )
    facets = DownloadableFiles.get_data_category_facets(
        filter_=DownloadableFiles.build_file_filter(
            trial_ids=args.get("trial_ids"), user=user
        )
    )
    return jsonify({"trial_ids": trial_ids, "facets": facets})
