from flask import Blueprint, jsonify

from ..csms import get_with_authorization as csms_get
from ..models import CIDCRole, syncall_from_blobs
from ..shared.auth import requires_auth
from ..shared.gcloud_client import _encode_and_publish
from ..config.settings import GOOGLE_GRANT_DOWNLOAD_PERMISSIONS_TOPIC

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

    report = _encode_and_publish(str({}), GOOGLE_GRANT_DOWNLOAD_PERMISSIONS_TOPIC)

    # Wait for response from pub/sub
    if report:
        report.result()

    res = jsonify(status="success")
    res.status_code = 200
    return res
