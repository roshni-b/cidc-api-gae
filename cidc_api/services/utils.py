from functools import wraps

from eve.auth import requires_auth
from flask import current_app as app


def resource(
    name: str,
    public_methods: list = [],
    allowed_roles: list = [],
    allowed_read_roles: list = [],
    allowed_write_roles: list = [],
):
    """
    Decorator for custom endpoints that adds and configures that endpoint
    as a resource in the Eve API's DOMAIN config.
    """

    def decorator(f):
        @wraps(f)
        def wrapped(*args, **kwargs):
            kwargs["resource"] = name
            app.config["DOMAIN"][name] = {
                "public_methods": public_methods,
                "allowed_roles": allowed_roles,
                "allowed_read_roles": allowed_read_roles,
                "allowed_write_roles": allowed_write_roles,
                "authentication": app.auth,
            }

            @requires_auth("resource")
            def f_w_args(*args, **kwargs):
                return f()

            return f_w_args(*args, **kwargs)

        return wrapped

    return decorator
