import io
from datetime import datetime

from flask import Blueprint, send_file
from werkzeug.exceptions import BadRequest

from ..shared.auth import get_current_user, requires_auth
from ..shared.emails import new_user_registration
from ..shared.rest_utils import (
    with_lookup,
    marshal_response,
    unmarshal_request,
    use_args_with_pagination,
)
from ..models import (
    Users,
    UserSchema,
    UserListSchema,
    CIDCRole,
    IntegrityError,
    Permissions,
)
from ..config.settings import ENV

users_bp = Blueprint("users", __name__)

user_schema = UserSchema()
user_list_schema = UserListSchema()
new_user_schema = UserSchema(exclude=("approval_date", "role", "disabled"))
partial_user_schema = UserSchema(partial=True)


@users_bp.route("/self", methods=["GET"])
@requires_auth("self")
@marshal_response(user_schema)
def get_self():
    """Return the current user's information to them."""
    return get_current_user()


@users_bp.route("/self", methods=["POST"])
@requires_auth("self")
@unmarshal_request(new_user_schema, "user")
@marshal_response(user_schema, 201)
def create_self(user):
    """
    Allow the current user to create a profile for themself. On success,
    send an email to the CIDC mailing list with a registration notification.
    """
    current_user = get_current_user()

    if current_user.email != user.email:
        raise BadRequest(
            f"{current_user.email} can't create a user with email {user.email}"
        )

    try:
        user.insert()
    except IntegrityError as e:
        raise BadRequest(str(e.orig))

    new_user_registration(user.email, send_email=True)

    return user


@users_bp.route("/", methods=["POST"])
@requires_auth("users", [CIDCRole.ADMIN.value])
@unmarshal_request(user_schema, "user")
@marshal_response(user_schema, 201)
def create_user(user):
    """
    Allow admins to create user records.
    """
    try:
        user.insert()
    except IntegrityError as e:
        raise BadRequest(str(e.orig))

    return user


@users_bp.route("/", methods=["GET"])
@requires_auth("users", [CIDCRole.ADMIN.value])
@use_args_with_pagination({}, user_schema)
@marshal_response(user_list_schema)
def list_users(args, pagination_args):
    """
    List all users. TODO: pagination support
    """
    users = Users.list(**pagination_args)
    count = Users.count()
    return {"_items": users, "_meta": {"total": count}}


@users_bp.route("/<int:user>", methods=["GET"])
@requires_auth("users_item", [CIDCRole.ADMIN.value])
@with_lookup(Users, "user")
@marshal_response(user_schema)
def get_user(user: Users):
    """Get a single user by their id."""
    # this is not user-input due to @with_lookup, so safe to return
    return user


@users_bp.route("/<int:user>", methods=["PATCH"])
@requires_auth("users_item", [CIDCRole.ADMIN.value])
@with_lookup(Users, "user", check_etag=True)
@unmarshal_request(partial_user_schema, "user_updates", load_sqla=False)
@marshal_response(user_schema)
def update_user(user: Users, user_updates: Users):
    """Update a single user's information."""
    # If a user is being awarded their first role, add an approval date
    if not user.role and "role" in user_updates:
        user_updates["approval_date"] = datetime.now()

    # If this user is being re-enabled after being disabled, update their last
    # access date to now so that they aren't disabled again tomorrow and
    # refresh their IAM permissions.
    if user.disabled and user_updates.get("disabled") == False:
        user_updates["_accessed"] = datetime.now()
        Permissions.grant_iam_permissions(user)

    user.update(changes=user_updates)

    # this is not user-input due to @with_lookup, so safe to return
    return user


@users_bp.route("/data_access_report", methods=["GET"])
@requires_auth("users_data_access_report", [CIDCRole.ADMIN.value])
def get_data_access_report():
    """Generate the user data access report."""
    buffer = io.BytesIO()
    Users.get_data_access_report(buffer)
    buffer.seek(0)

    filename = f"cidc_{ENV}_data_access_{datetime.now().date()}.xlsx"

    return send_file(buffer, as_attachment=True, attachment_filename=filename)
