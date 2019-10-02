from eve import Eve

from flask import _request_ctx_stack, Blueprint, Request

from models import CIDCRole


def register_permissions_hooks(app: Eve):
    app.on_pre_GET_permissions = update_permissions_filters


def update_permissions_filters(request: Request, lookup: dict):
    """
    Update a GET request to the 'permissions' resource to include only
    the current user's permissions, unless the user is an admin.
    """
    user = _request_ctx_stack.top.current_user

    # Admins can get all permissions
    if user.role == CIDCRole.ADMIN.value:
        return

    # Otherwise, only include permissions granted to this user
    lookup["to_user"] = user.id
