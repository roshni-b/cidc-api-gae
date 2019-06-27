import json
from logging import getLogger
from typing import List, Tuple

import requests
from eve.auth import TokenAuth
from flask import current_app as app, _request_ctx_stack
from jose import jwt

from models import Users

ALGORITHMS = ["RS256"]

# TODO: better error handling all around


class BearerAuth(TokenAuth):
    """
    Handles bearer token authorization.
    """

    def check_auth(self, token, allowed_roles, resource, method):
        """
        Validates the user's access token.
        Arguments:
            token {str} -- a JWT access token.
            allowed_roles {List[str]} -- Array of strings of user roles.
            resource {str} -- Endpoint being accessed.
            method {str} -- HTTP method (GET, POST, PATCH, DELETE)
        """
        try:
            # Attempt to obtain the current user's email from the access token.
            # TODO: implement token caching. Checking with Auth0 every time is very slow.
            email = get_user_email(token)

            assert email

            Users.find_or_create(email)

            # Authentication succeeded
            return True
        except Exception as e:
            # TODO: be more selective about what errors constitute an auth error,
            # leading to a rejection of the auth attempt, versus a server error.
            app.logger.error(e)

            # Authentication failed
            return False


def validate_payload(token: str, rsa_key: dict, audience: str) -> dict:
    """
    Decodes the token and checks it for validity.
    Arguments:
        token {str} -- JWT
        rsa_key {dict} -- rsa_key
        audience {str} -- parameter to use as the audience.
    Returns:
        dict -- Decoded token as a dictionary.
    """
    # TODO: handle expiration and claims errors

    return jwt.decode(
        token,
        rsa_key,
        algorithms=ALGORITHMS,
        audience=audience,
        issuer="https://%s/" % app.config["AUTH0_DOMAIN"],
        options={
            "verify_signature": True,
            "verify_aud": True,
            "verify_iat": True,
            "verify_exp": True,
            "verify_nbf": True,
            "verify_iss": True,
            "verify_sub": True,
            "verify_jti": True,
            "verify_at_hash": False,
            "leeway": 0,
        },
    )


def get_user_email(token):
    """
    Checks if the supplied token is valid.
    Arguments:
        token {str} -- JWT token.
    Raises:
        AuthError -- [description]
    Returns:
        str -- Authorized user's email.
    """
    unverified_header = jwt.get_unverified_header(token)

    # Get public keys from our Auth0 domain
    jwks_url = "https://%s/.well-known/jwks.json" % app.config["AUTH0_DOMAIN"]
    jwks = requests.get(jwks_url).json()

    # Obtain the public key used to sign this token
    rsa_key = None
    for key in jwks["keys"]:
        if key["kid"] == unverified_header["kid"]:
            rsa_key = {
                "kty": key["kty"],
                "kid": key["kid"],
                "use": key["use"],
                "n": key["n"],
                "e": key["e"],
            }

    # If no matching public key was found, we can't validate the token
    assert rsa_key

    # Validate that the provided token was issued by Auth0
    payload = validate_payload(token, rsa_key, app.config["AUTH0_AUDIENCE"])

    # Use the obtained authorization token to access user info
    userinfo_url = "https://%s/userinfo" % app.config["AUTH0_DOMAIN"]
    userinfo = requests.get(
        userinfo_url, headers={"Authorization": f"Bearer {token}"}
    ).json()

    _request_ctx_stack.top.current_user = userinfo
    app.logger.info("Authenticated user: " + userinfo["email"])

    return userinfo["email"]
