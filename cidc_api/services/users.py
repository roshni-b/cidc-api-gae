"""Endpoints for user account operations"""
from flask import Blueprint, jsonify, abort, Request, _request_ctx_stack
from eve import Eve
from eve.auth import requires_auth
from werkzeug.exceptions import Unauthorized, BadRequest


def register_users_hooks(app: Eve):
    app.on_pre_POST_users += enforce_self_creation


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
