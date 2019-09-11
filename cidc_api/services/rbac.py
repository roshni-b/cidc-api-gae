"""Hooks and endpoints for orchestrating Role-Based Access Control"""
import json

from eve import Eve
from flask import Request, _request_ctx_stack
from werkzeug.datastructures import ImmutableMultiDict
from models import Users, Permissions


def register_rbac_hooks(app: Eve):
    app.on_pre_GET_downloadable_files = update_filters


def update_filters(request: Request, _):
    """Modify a lookup to filter out values a user doesn't have permission to see."""
    user = _request_ctx_stack.top.current_user

    # Ensure user is registered. Auth should prevent this from ever throwing.
    assert user.approval_date and user.role

    permissions = Permissions.find_for_user(user)

    lookup_str = request.args.get("where")
    lookup = json.loads(lookup_str) if lookup_str else {}
    for field in ["trial_id", "assay_type"]:
        requested = lookup.get(field, [])
        if type(requested) != list:
            requested = [requested]
        allowed = set(getattr(perm, field) for perm in permissions)
        query = allowed if requested else allowed.intersection(requested)
        lookup[field] = list(query)

    request.args = ImmutableMultiDict(lookup)

    print(lookup)
