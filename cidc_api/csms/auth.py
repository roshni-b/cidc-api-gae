from datetime import datetime, timedelta
from ..config.logging import get_logger
import requests

from ..config.settings import (
    CSMS_BASE_URL,
    CSMS_CLIENT_ID,
    CSMS_CLIENT_SECRET,
    CSMS_TOKEN_URL,
)


logger = get_logger(__name__)
_TOKEN, _TOKEN_EXPIRY = None, datetime.now()


def get_token():
    global _TOKEN, _TOKEN_EXPIRY
    if not _TOKEN or datetime.now() >= _TOKEN_EXPIRY:
        res, time = (
            requests.post(
                CSMS_TOKEN_URL,
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                data={
                    "grant_type": "client_credentials",
                    "client_id": CSMS_CLIENT_ID,
                    "client_secret": CSMS_CLIENT_SECRET,
                },
            ).json(),
            datetime.now(),
        )

        # res definition from https://developer.okta.com/docs/reference/api/oidc/#response-example-error-7
        if "errorCode" in res:
            raise Exception(res["errorCode"] + ": " + res.get("errorSummary"))
        
        _TOKEN = res["access_token"]
        _TOKEN_EXPIRY = time + timedelta(seconds=res["expires_in"])

    return _TOKEN


def get_with_authorization(url: str, *args, **kwargs):
    """url should be fully valid or begin with `/` to be prefixed with CSMS_BASE_URL"""
    token = get_token()
    headers = {
        **kwargs.get("headers", {}),
        "Authorization": f"Bearer {token}",
        "accept": "*/*",
    }
    kwargs["headers"] = headers
    if not url.startswith(CSMS_BASE_URL):
        url = CSMS_BASE_URL + url
    return requests.get(url, *args, **kwargs)
