from flask import Blueprint, jsonify

from ..models import CIDCRole, Permissions, syncall_from_blobs
from ..shared.auth import requires_auth
from ..csms import get_with_authorization as csms_get

admin_bp = Blueprint("admin", __name__)


@admin_bp.route("/test_csms", methods=["GET"])
@requires_auth("admin", [CIDCRole.ADMIN.value])
def test_csms():
    return csms_get("/doc").json()


@admin_bp.route("/load_from_blobs", methods=["GET"])
@requires_auth("admin", [CIDCRole.ADMIN.value])
def load_from_blobs():
    errors = syncall_from_blobs()
    if len(errors):
        res = jsonify(errors=[str(e) for e in errors])
        res.status_code = 500
        return res
    else:
        res = jsonify(status="success")
        res.status_code = 200
        return res


@admin_bp.route("/grant_all_download_permissions", methods=["GET"])
@requires_auth("admin", CIDCRole.ADMIN.value)
def grant_all_download_permissions():
    try:
        Permissions.grant_all_download_permissions()
    except Exception as e:
        res = jsonify(error=str(e))
        res.status_code = 500
    else:
        res = jsonify(status="success")
        res.status_code = 200
    finally:
        return res
