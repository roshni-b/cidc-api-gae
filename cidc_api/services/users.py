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

import gcloud_client
from models import Users, CIDCRole
from emails import new_user_registration, confirm_account_approval


def register_users_hooks(app: Eve):
    # new_users hooks
    app.on_pre_POST_new_users += enforce_self_creation
    app.on_inserted_new_users += alert_new_user_registered

    # users hooks
    app.on_pre_GET_users += filter_user_lookup
    app.on_pre_PATCH_users += add_approval_date
    app.on_update_users += alert_new_user_approved


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


def alert_new_user_registered(items: list):
    """
    When a new user has registered, send an email to the CIDC admins alerting them
    """
    assert len(items) == 1

    new_user = items[0]
    email = new_user_registration(new_user["email"])

    gcloud_client.send_email(**email)


def filter_user_lookup(request: Request, lookup: dict):
    """
    Ensure that non-admin users can only look up their own account info.
    """
    current_user = _request_ctx_stack.top.current_user

    # If user isn't an admin, they can only lookup their own info.
    if current_user.role != CIDCRole.ADMIN.value:
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


def alert_new_user_approved(updates: dict, original: dict):
    """
    If a new user was just approved, send them an email notification.
    """
    if "approval_date" in updates:
        # The user was just approved, so ping them.
        if updates["approval_date"] and not original.get("approval_date"):
            user = Users(**original)
            email = confirm_account_approval(user)
            gcloud_client.send_email(**email)
