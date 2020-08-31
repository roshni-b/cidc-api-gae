from typing import List

from webargs import fields
from webargs.flaskparser import use_args
from flask import Blueprint
from werkzeug.exceptions import Unauthorized, NotFound, BadRequest, InternalServerError

from ..models import (
    Permissions,
    PermissionSchema,
    PermissionListSchema,
    CIDCRole,
    IntegrityError,
    NoResultFound,
    IAMException,
)
from ..shared.auth import get_current_user, requires_auth
from ..shared.rest_utils import (
    with_lookup,
    marshal_response,
    unmarshal_request,
    delete_response,
)

permissions_bp = Blueprint("permissions", __name__)

permission_schema = PermissionSchema()
permission_list_schema = PermissionListSchema()


@permissions_bp.route("/", methods=["GET"])
@requires_auth("permissions")
@use_args({"user_id": fields.Int()}, location="query")
@marshal_response(permission_list_schema)
def list_permissions(args: dict):
    """
    List all permissions for the current user, unless the `user_id` query param is provided.
    If the `user_id` query param is provided and the current user is an admin, then list
    all of the permissions granted for the user with id `user_id`.
    """
    current_user = get_current_user()
    user_id = args.get("user_id")

    # Admins can look up permissions for any user, but
    # non-admins can only look up their own permissions
    if current_user.is_admin():
        if user_id:
            permissions = Permissions.find_for_user(user_id)
        else:
            permissions = Permissions.list()
    else:
        if user_id and current_user.id != user_id:
            raise Unauthorized(
                f"{current_user.email} cannot view permissions for other users"
            )
        permissions = Permissions.find_for_user(current_user.id)

    # Since we aren't paginating, `permissions` is a list of all requested permissions
    total = len(permissions)

    return {"_items": permissions, "_meta": {"total": total}}


@permissions_bp.route("/<int:permission>", methods=["GET"])
@requires_auth("permissions_item")
@with_lookup(Permissions, "permission")
@marshal_response(permission_schema)
def get_permission(permission: Permissions) -> Permissions:
    """Look up the permission record with id `permission_id`."""
    current_user = get_current_user()

    # If the user isn't allowed to view this permission, respond with 404.
    if not current_user.is_admin() and permission.granted_to_user != current_user.id:
        raise NotFound()

    return permission


@permissions_bp.route("/", methods=["POST"])
@requires_auth("permissions_item", allowed_roles=[CIDCRole.ADMIN.value])
@unmarshal_request(permission_schema, "permission")
@marshal_response(permission_schema, 201)
def create_permission(permission: Permissions) -> Permissions:
    """Create a new permission record."""
    granter = get_current_user()
    permission.granted_by_user = granter.id
    try:
        permission.insert()
    except IntegrityError as e:
        raise BadRequest(str(e.orig))
    except IAMException as e:
        # We return info on this internal error, since this is an admin-only endpoint
        raise InternalServerError(str(e))

    return permission


@permissions_bp.route("/<int:permission>", methods=["DELETE"])
@requires_auth("permissions_item", allowed_roles=[CIDCRole.ADMIN.value])
@with_lookup(Permissions, "permission", check_etag=True)
def delete_permission(permission: Permissions):
    """Delete a permission record."""
    try:
        permission.delete()
    except NoResultFound as e:
        raise NotFound(str(e.orig))
    except IAMException as e:
        # We return info on this internal error, since this is an admin-only endpoint
        raise InternalServerError(str(e))

    return delete_response()
