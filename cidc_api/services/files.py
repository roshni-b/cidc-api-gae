"""Endpoints for downloadable file operations"""
import ast
import json
from multiprocessing.pool import ThreadPool

from eve import Eve
from eve_sqlalchemy import parse
from flask import Request, request, _request_ctx_stack, Blueprint, jsonify
from werkzeug.exceptions import BadRequest, NotFound
from werkzeug.datastructures import ImmutableMultiDict

import gcloud_client
from auth import requires_auth
from models import Users, Permissions, CIDCRole, DownloadableFiles

files_api = Blueprint("files", __name__, url_prefix="/downloadable_files")


@files_api.route("/download_url", methods=["GET"])
@requires_auth("download_url")
def get_download_url():
    """
    Get a signed GCS download URL for a given file.
    """
    # Extract file ID from route
    file_id = request.args.get("id")
    if not file_id:
        raise BadRequest("Missing expected URL parameter `id`")

    # Check that file exists
    file_record = DownloadableFiles.find_by_id(file_id)
    if not file_record:
        raise NotFound(f"No file with id {file_id}.")

    user = _request_ctx_stack.top.current_user

    # Ensure user has permission to access this file
    perms = Permissions.find_for_user(user)
    if user.role != CIDCRole.ADMIN.value:
        # Check for a permission matching this file's trial and assay
        if not any(
            perm.upload_type == file_record.upload_type
            and perm.trial_id == file_record.trial_id
            for perm in perms
        ):
            raise NotFound(f"No file with id {file_id}.")

    # Generate the signed URL and return it.
    download_url = gcloud_client.get_signed_url(file_record.object_url)
    return jsonify(download_url)


@files_api.route("/filter_facets", methods=["GET"])
@requires_auth("filter_facets")
def get_filter_facets():
    """
    Return a list of allowed filter facet values for a user.
    Response will have structure:
    {
        <facet 1>: [<value 1>, <value 2>,...],
        <facet 2>: [...],
        ...
    }
    """
    user = _request_ctx_stack.top.current_user

    if user.role == CIDCRole.ADMIN.value:
        # Admins can facet on every trial or upload type
        trial_ids = DownloadableFiles.get_distinct("trial_id")
        upload_types = DownloadableFiles.get_distinct("upload_type")
    else:
        # Non-admins can only facet on what they have permission to view
        perms = Permissions.find_for_user(user)
        trial_ids = list({perm.trial_id for perm in perms})
        upload_types = list({perm.upload_type for perm in perms})

    return jsonify({"trial_id": trial_ids, "upload_type": upload_types})


def register_files_hooks(app: Eve):
    app.on_pre_GET_downloadable_files = update_file_filters


def update_file_filters(request: Request, _):
    """
    Modify a downloadable_files lookup to filter out files 
    a user doesn't have permission to see.
    """
    user = _request_ctx_stack.top.current_user

    # Admins can do whatever they want
    if user.role == CIDCRole.ADMIN.value:
        return

    permissions = Permissions.find_for_user(user)

    # User cannot access any trials, so make filter guaranteed-empty
    if len(permissions) == 0:
        guaranteed_empty = "(trial==a and trial==b)"
        lookup = request.args.copy()
        lookup["where"] = guaranteed_empty
        request.args = ImmutableMultiDict(lookup)
        return

    # Build a where-query that looks up only the downloadable_files with
    # trial ID and upload type that the current user is allowed to view.
    # If the user has permission to view WES for Trial "1" and Olink for "2",
    # this query will look like:
    #   (trial==1 and upload_type==wes)or(trial==2 and upload_type==olink)
    where_query = "or".join(
        f"(trial=={p.trial_id!r} and upload_type=={p.upload_type!r})"
        for p in permissions
    )

    user_where_query = request.args.get("where")
    if user_where_query:
        # Check that the filter is not Mongo-style (i.e., JSON). Even though
        # Eve-SQLAlchemy supports this syntax, we don't have a good way to combine
        # JSON-style filters with the permissions where_query
        try:
            mongo_style_filter = json.loads(user_where_query)
            raise BadRequest(
                "Mongo-style JSON filters are not supported on the downloadable_files table. "
                "Use SQL-style filters with the form "
                "'<field1> == <value1> or (<field1> == <value2> and <field3> = <value3>)"
            )
        except json.JSONDecodeError:
            # The user provided a non-JSON filter, which is what we want
            pass

        try:
            # Check that the filter is a valid filter (e.g., has balanced parentheses).
            # We set mode to "eval" to compile the string to an expression.
            mod = ast.parse(user_where_query, mode="eval")
        except:
            raise BadRequest(f"Could not parse filter {where_query!r}")

        # Add permission filters to the user's where query in an "and" clause
        where_query = f"({user_where_query})and({where_query})"

    lookup = request.args.copy()
    lookup["where"] = where_query
    request.args = ImmutableMultiDict(lookup)
