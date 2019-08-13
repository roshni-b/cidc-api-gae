"""Endpoints for user account operations"""
import json
from datetime import datetime

from flask import (
    Blueprint,
    jsonify,
    abort,
    Request,
    _request_ctx_stack,
    current_app as app,
)
from eve import Eve
from eve.auth import requires_auth
from werkzeug.exceptions import Unauthorized, BadRequest

from models import Users


def register_users_hooks(app: Eve):
    app.on_pre_POST_new_users += enforce_self_creation
    app.on_pre_GET_users += filter_user_lookup
    app.on_pre_PATCH_users += add_approval_date


def enforce_self_creation(request: Request):
    """
    Ensures the request's current user can only create themself.

    If a user is trying to create a user record with an email other
    than their own, respond with a 401 error.
    """
    payload = request.json
    current_user = _request_ctx_stack.top.current_user

    if "email" not in payload:
        raise BadRequest(f"Cannot create user. Users must have an 'email'.")

    if payload["email"] != current_user.email:
        raise Unauthorized(
            f"{current_user.email} not authorized to create user with email '{payload['email']}'."
        )


def filter_user_lookup(request: Request, lookup: dict):
    """
    Ensure that non-admin users can only look up their own account info.
    """
    current_user = _request_ctx_stack.top.current_user

    # If user isn't an admin, they can only lookup their own info.
    if current_user.role != "cidc-admin":
        lookup["email"] = current_user.email


def add_approval_date(request: Request, lookup: dict):
    """
    When a user's role is set for the first time, also set their approval date.
    """
    user_patch = request.json

    if "role" in user_patch:
        user_id = request.view_args["id"]
        user_record = Users.find_by_id(user_id)
        if user_record.role is None:
            request.json["approval_date"] = datetime.now()
