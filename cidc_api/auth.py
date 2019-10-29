import logging
from packaging import version
from functools import wraps
from typing import List

import requests
from eve.auth import TokenAuth
from jose import jwt
from flask import _request_ctx_stack, request, current_app as app
from werkzeug.exceptions import Unauthorized, BadRequest, PreconditionFailed

from models import Users
from config.settings import AUTH0_DOMAIN, ALGORITHMS, AUTH0_CLIENT_ID, TESTING


def requires_auth(resource: str, allowed_roles: list = []):
    """
    A decorator that adds authentication and basic role-based access to a custom endpoint.

    A workaround for the issues with eve.auth.requires_auth: 
    https://github.com/pyeve/eve/issues/860

    NOTE: leaving the `allowed_roles` argument empty allows any authenticated user to access
    the decorated endpoint.
    """

    def decorator(f):
        @wraps(f)
        def wrapped(*args, **kwargs):
            app.auth.authorized(allowed_roles, resource, request.method)
            return f(*args, **kwargs)

        return wrapped

    return decorator


class BearerAuth(TokenAuth):
    """
    Handles bearer token authorization.
    """

    def check_auth(
        self, id_token: str, allowed_roles: List[str], resource: str, method: str
    ) -> bool:
        """
        Validates the user's id_token, extracts user info if valid.
        If this is a registration attempt, create 

        Args:
            id_token: A JWT id_token
            allowed_roles: Array of user roles allowed to act on this resource
            resource: Endpoint being accessed
            method: HTTP method (GET, POST, PATCH, DELETE)
        
        Returns:
            bool: True if the user successfully authenticated, False otherwise.
        
        TODO: role-based resource/method-level authorization
        """
        profile = self.token_auth(id_token)

        def log_user_and_request_details(is_authorized: bool):
            """Log user and request info before every request"""
            log_msg = f"{'' if is_authorized else 'UN'}AUTHORIZED"

            # log request details
            log_msg += (
                f" {request.environ['REQUEST_METHOD']} {request.environ['RAW_URI']}"
            )

            # log user details
            user = _request_ctx_stack.top.current_user
            log_msg += f" (user:{user.id}:{user.email})"

            print(log_msg)

        try:
            is_authorized = self.role_auth(profile, allowed_roles, resource, method)
        except Unauthorized:
            log_user_and_request_details(False)
            raise

        log_user_and_request_details(is_authorized)

        self.enforce_cli_version()

        return is_authorized

    def enforce_cli_version(self):
        """
        If the current request appears to come from the CLI and not the Portal, enforce the configured
        minimum CLI version.
        """
        user_agent = request.headers.get("User-Agent")

        # e.g., during testing no User-Agent header is supplied
        if not user_agent:
            return

        try:
            client, client_version = user_agent.split("/", 1)
        except ValueError:
            print(f"Unrecognized user-agent string format: {user_agent}")
            raise BadRequest("could not parse User-Agent string")

        # Old CLI versions don't update the User-Agent header, so we (perhaps dangerously)
        # assume any request coming from the python requests library is from a "very" old
        # version of the CLI.
        is_very_old_cli = client == "python-requests"

        # Newer version of the CLI update the User-Agent header to `cidc-cli/{version}`,
        # so we can assess whether the requester needs to update their CLI.
        is_old_cli = client == "cidc-cli" and version.parse(
            client_version
        ) < version.parse(app.config["MIN_CLI_VERSION"])

        if is_very_old_cli or is_old_cli:
            print("cancelling request: detected outdated CLI")
            message = (
                "You appear to be using an out-of-date version of the CIDC CLI. "
                "Please upgrade to the most recent version:\n"
                "    pip3 install --upgrade cidc-cli"
            )
            if is_very_old_cli:
                # This is semantically incorrect, but there is no other way
                # to get the error message to show up for the oldest versions of the CLI
                raise Unauthorized(message)
            else:
                raise PreconditionFailed(message)

    def role_auth(
        self, profile: dict, allowed_roles: List[str], resource: str, method: str
    ) -> bool:
        """Check if the current user is authorized to act on the current request's resource."""
        user = Users.find_by_email(profile["email"])
        _request_ctx_stack.top.current_user = user

        # User hasn't registered yet.
        if not user:
            # Although the user doesn't exist in the database, we still
            # make the user's identity data available in the request context.
            _request_ctx_stack.top.current_user = Users(email=profile["email"])

            # User is only authorized to create themself.
            if resource == "new_users" and method == "POST":
                return True

            raise Unauthorized(f'{profile["email"]} is not registered.')

        # User is registered but disabled.
        if user.disabled:
            # Disabled users are not authorized to do anything but access their
            # account info.
            if resource == "self" and method == "GET":
                return True

            raise Unauthorized(f'{profile["email"]}\'s account is disabled.')

        # User is registered but not yet approved.
        if not user.approval_date:
            # Unapproved users are not authorized to do anything but access their
            # account info.
            if resource == "self" and method == "GET":
                return True

            raise Unauthorized(
                f'{profile["email"]}\'s registration is pending approval'
            )

        # User is approved and registered, so just check their role.
        if allowed_roles and user.role not in allowed_roles:
            raise Unauthorized(
                f'{profile["email"]} is not authorized to access this endpoint.'
            )

        return True

    def token_auth(self, id_token: str) -> dict:
        """
        Checks if the supplied id_token is valid, and, if so,
        decodes and returns its payload.

        Args:
            id_token: a JWT id_token.

        Returns:
            dict: the user's decoded profile info.

        TODO: implement token caching
        """
        public_key = self.get_issuer_public_key(id_token)

        payload = self.decode_id_token(id_token, public_key)

        return payload

    def get_issuer_public_key(self, token: str) -> dict:
        """
        Get the appropriate public key to check this token for authenticity.

        Args:
            token: an encoded JWT.
        
        Raises:
            Unauthorized: if no public key can be found.
            
        Returns:
            str: the public key.
        """
        try:
            header = jwt.get_unverified_header(token)
        except jwt.JWTError as e:
            raise Unauthorized(str(e))

        # Get public keys from our Auth0 domain
        jwks_url = f"https://{AUTH0_DOMAIN}/.well-known/jwks.json"
        jwks = requests.get(jwks_url).json()

        # Obtain the public key used to sign this token
        public_key = None
        for key in jwks["keys"]:
            if key["kid"] == header["kid"]:
                public_key = key

        # If no matching public key was found, we can't validate the token
        if not public_key:
            raise Unauthorized("Found no public key with id %s" % header["kid"])

        return public_key

    def decode_id_token(self, token: str, public_key: dict) -> dict:
        """
        Decodes the token and checks it for validity.

        Args:
            token: the JWT to validate and decode
            public_key: public_key

        Raises:
            Unauthorized: 
                - if token is expired
                - if token has invalid claims
                - if token signature is invalid in any way

        Returns:
            dict: the decoded token as a dictionary.
        """
        try:
            payload = jwt.decode(
                token,
                public_key,
                algorithms=ALGORITHMS,
                # TODO: is this what we want?
                audience=AUTH0_CLIENT_ID,
                issuer=f"https://{AUTH0_DOMAIN}/",
                options={"verify_at_hash": False},
            )
        except jwt.ExpiredSignatureError as e:
            raise Unauthorized(f"{e}. Try log in again with cidc login.")
        except jwt.JWTClaimsError as e:
            raise Unauthorized(str(e))
        except jwt.JWTError as e:
            raise Unauthorized(str(e))

        # Currently, only id_tokens are accepted for authentication.
        # Going forward, we could also accept access tokens that we
        # use to query the userinfo endpoint.
        if "email" not in payload:
            msg = "An id_token with an 'email' field is required to authenticate"
            raise Unauthorized(msg)

        return payload
