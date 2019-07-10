from flask import jsonify, Response
from eve import Eve


def register_error_handlers(app: Eve):
    """Register error handlers on an Eve app"""
    app.register_error_handler(ServerError, ServerError.handle_error)
    app.register_error_handler(AuthError, AuthError.handle_error)


class ServerError(ValueError):
    """A generic error that the API server knows how to handle"""

    # 500 Internal Server Error
    status_code = 500

    def __init__(self, error_code: str, message: str):
        """
        Build an error with client-friendly info about itself.

        Args:
            error_code: an error code, e.g., 'token_expired'
            message: a human-friendly description of the error
        """
        self.error_code = error_code
        self.message = message

    def json(self):
        return {"error_code": self.error_code, "message": self.message}

    @classmethod
    def handle_error(cls, e) -> Response:
        """Generate a Flask response when a ServerError (or subclass) is thrown"""
        assert isinstance(e, cls), "%s expected, but received %s" % (
            cls.__class__,
            e.__class__,
        )

        # Convert the error to a Flask response object
        response = jsonify(e.json())
        response.status_code = e.status_code

        return response


class AuthError(ServerError):
    """An error resulting in failure to authenticate a user"""

    # 401 Unauthorized
    status_code = 401
