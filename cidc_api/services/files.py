"""Endpoints for downloadable file operations"""
import ast
import json
from multiprocessing.pool import ThreadPool

from eve import Eve
from eve_sqlalchemy import parse
from flask import Request, _request_ctx_stack
from werkzeug.exceptions import BadRequest
from werkzeug.datastructures import ImmutableMultiDict

import gcloud_client
from models import Users, Permissions, CIDCRole


def register_files_hooks(app: Eve):
    #     app.on_fetched_resource_downloadable_files += insert_download_urls
    app.on_fetched_item_downloadable_files += insert_download_url
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
    # trial ID and assay type that the current user is allowed to view.
    # If the user has permission to view WES for Trial "1" and Olink for "2",
    # this query will look like:
    #   (trial==1 and assay_type==wes)or(trial==2 and assay_type==olink)
    where_query = "or".join(
        f"(trial=={p.trial_id!r} and assay_type=={p.assay_type!r})" for p in permissions
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


def insert_download_url(payload: dict):
    """
    Get a signed GCS download URL for the requested file.

    TODO: evaluate ways of caching signed URLs for files that haven't yet
    expired, so that we aren't re-generating them on every request.
    """
    object_url = payload["object_url"]
    download_url = gcloud_client.get_signed_url(object_url)
    payload["download_link"] = download_url


def insert_download_urls(payload: dict):
    """
    Get a signed GCS download URL for each of the requested files.

    NOTE: this hook is currently disabled.
    """
    # Each call to insert_download_url generates a request
    # to the GCS API, so we get a speed-up from multithreading:
    # although Python dispatches the requests synchronously,
    # the OS handles the network requests in parallel.
    with ThreadPool() as pool:
        pool.map(insert_download_url, payload["_items"])
