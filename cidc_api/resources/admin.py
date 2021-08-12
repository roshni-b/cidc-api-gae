from flask import Blueprint

from ..models import CIDCRole, syncall_from_blobs
from ..shared.auth import get_current_user, requires_auth
from ..csms import get_with_authorization as csms_get

admin_bp = Blueprint("admin", __name__)


@admin_bp.route("/test_csms", methods=["GET"])
@requires_auth("csms", [CIDCRole.ADMIN.value])
def test_csms():
    return csms_get("/doc").json()


@admin_bp.route("/load_from_blobs", methods=["GET"])
@requires_auth("csms", [CIDCRole.ADMIN.value])
def load_from_blobs():
    errs = syncall_from_blobs()
    if len(errs):
        return f"Error: {errs}", 500
    else:
        return "Success", 200
