import pytest
from flask import Flask
from werkzeug.exceptions import BadRequest, UnprocessableEntity

from cidc_api.shared.rest_utils import unmarshal_request, marshal_response
from cidc_api.models import PermissionSchema, PermissionListSchema, Permissions


def assert_sqla_matching_fields(rec1, rec2):
    received = rec1.__dict__
    expected = rec2.__dict__
    received.pop("_sa_instance_state")
    expected.pop("_sa_instance_state")
    assert received == expected


perm_json = {
    "upload_type": "olink",
    "granted_by_user": 1,
    "granted_to_user": 2,
    "trial_id": "sometrial",
}
perm_record = Permissions(**perm_json)


def test_unmarshal_request(empty_app: Flask):
    """Check that unmarshal_request validates and loads request JSON as expected."""
    s, p, a = 1, 2, 3

    @unmarshal_request(PermissionSchema(), "permission_record", load_sqla=True)
    def endpoint_sqla(some, positional, args, permission_record):
        assert some == s and positional == p and args == a
        return permission_record

    @unmarshal_request(PermissionSchema(), "permission_record", load_sqla=False)
    def endpoint_json(some, positional, args, permission_record):
        assert some == s and positional == p and args == a
        return permission_record

    # A request with no JSON body should raise 400
    with empty_app.test_request_context():
        with pytest.raises(BadRequest, match="expected JSON data"):
            endpoint_sqla(s, p, a)

        with pytest.raises(BadRequest, match="expected JSON data"):
            endpoint_json(s, p, a)

    # Invalid JSON should raise 422
    with empty_app.test_request_context(json={"granted_to_user": 1}):
        with pytest.raises(
            UnprocessableEntity, match="Missing data for required field"
        ):
            endpoint_sqla(s, p, a)

        with pytest.raises(
            UnprocessableEntity, match="Missing data for required field"
        ):
            endpoint_json(s, p, a)

    # Valid JSON should be happily accepted
    with empty_app.test_request_context(json=perm_json):
        assert_sqla_matching_fields(endpoint_sqla(s, p, a), perm_record)
        assert endpoint_json(s, p, a) == perm_json


def test_marshal_response(empty_app):
    """Check that marshal_response loads response JSON as expected."""
    marshal_permission = marshal_response(PermissionSchema())
    marshal_permissions = marshal_response(PermissionListSchema())

    s, p, a = 1, 2, 3

    # Test a well-behaved single-item endpoint
    @marshal_permission
    def endpoint(some, positional, args):
        assert some == s and positional == p and args == a
        return perm_record

    with empty_app.test_request_context():
        res = endpoint(s, p, a)
        assert res.json == perm_json

    # Test a well-behaved multi-item endpoint
    @marshal_permissions
    def endpoint(some, positional, args):
        assert some == s and positional == p and args == a
        return {"_items": [perm_record, perm_record], "_meta": {"total": 2}}

    with empty_app.test_request_context():
        res = endpoint(s, p, a)
        assert res.json["_items"] == [perm_json, perm_json]
        assert res.json["_meta"]["total"] == 2
