from cidc_api.models.models import CIDCRole
from io import BytesIO
from typing import List

from flask import Blueprint, jsonify, send_file
from webargs import fields
from webargs.flaskparser import use_args
from werkzeug.exceptions import NotFound, Unauthorized


from ..models import (
    DownloadableFiles,
    DownloadableFileSchema,
    DownloadableFileListSchema,
    Permissions,
    ROLES,
)
from ..shared import gcloud_client
from ..shared.auth import get_current_user, requires_auth
from ..shared.rest_utils import with_lookup, marshal_response, use_args_with_pagination
from ..config.settings import GOOGLE_DATA_BUCKET, MAX_PAGINATION_PAGE_SIZE

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
        return downloadable_file

    # Check that a non-admin has permission to view this file
    perm = Permissions.find_for_user_trial_type(
        user.id, downloadable_file.trial_id, downloadable_file.upload_type
    )

    if not perm:
        raise Unauthorized()

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


@downloadable_files_bp.route("/filelist", methods=["POST"])
@requires_auth("filelist")
@use_args({"file_ids": fields.List(fields.Int, required=True)}, location="json")
def generate_filelist(args):
    """
    Return a file `filelist.tsv` mapping GCS URIs to flat filenames for the
    provided set of file ids.
    """
    # Build the permissions filter
    current_user = get_current_user()
    user_perms_filter = DownloadableFiles.build_file_filter(user=current_user)

    # Get request object_urls
    urls = DownloadableFiles.list_object_urls(
        args["file_ids"], filter_=user_perms_filter
    )

    # If the query returned no files, respond with 404
    if len(urls) == 0:
        raise NotFound()

    # Build TSV mapping GCS URIs to flat filenames
    # (bytes because that's what send_file knows how to send)
    tsv = b""
    for url in urls:
        flat_url = url.replace("/", "_")
        full_gcs_uri = f"gs://{GOOGLE_DATA_BUCKET}/{url}"
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
