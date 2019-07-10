import pytest

from errors import ServerError, AuthError


def make_raiser(e: Exception):
    """Build a function that raises an exception e."""

    def raiser(*args, **kwargs):
        raise e

    return raiser


@pytest.mark.parametrize("e,status_code", [(AuthError, 401), (ServerError, 500)])
def test_basic_error_checks(e, status_code):
    """Basic server error sanity checks"""
    error_code, message = "a", "b"
    e = e(error_code, message)
    assert e.status_code == status_code
    assert e.json() == {"error_code": error_code, "message": message}
