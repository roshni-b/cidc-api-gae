"""Template functions for CIDC email bodies."""
from functools import wraps
from typing import Union

from cidc_api import gcloud_client
from cidc_api.models import Users, AssayUploads, ManifestUploads
from cidc_api.config.settings import ENV

CIDC_MAILING_LIST = "cidc@jimmy.harvard.edu"


def sendable(email_template):
    """
    Adds the `send` kwarg to an email template. If send_email=True, 
    send the email on function call.
    """

    @wraps(email_template)
    def wrapped(*args, send_email=False, **kwargs):
        email = email_template(*args, **kwargs)
        if send_email:
            gcloud_client.send_email(**email)
        return email

    return wrapped


@sendable
def confirm_account_approval(user: Users) -> dict:
    """Send a message to the user confirming that they are approved to use the CIDC."""

    subject = "CIDC Registration Approval"

    html_content = f"""
    <p>Hello {user.first_n} {user.last_n},</p>
    <p>
        Your registration for the CIMAC-CIDC Data Portal has now been approved. 
        To continue to the Portal, visit https://portal.cimac-network.org.
    </p>
    <p>If you have any questions, please email us at cidc@jimmy.harvard.edu.</p>
    <p>Thanks,<br/>The CIDC Project Team</p>
    """

    email = {
        "to_emails": [user.email],
        "subject": subject,
        "html_content": html_content,
    }

    return email


@sendable
def new_user_registration(email: str) -> dict:
    """Alert the CIDC admin mailing list to a new user registration."""

    subject = "New User Registration"

    html_content = (
        f"A new user, {email}, has registered for the CIMAC-CIDC Data Portal ({ENV}). If you are a CIDC Admin, "
        "please visit the accounts management tab in the Portal to review their request."
    )

    email = {
        "to_emails": [CIDC_MAILING_LIST],
        "subject": subject,
        "html_content": html_content,
    }

    return email


@sendable
def new_upload_alert(upload: Union[AssayUploads, ManifestUploads]) -> dict:
    """Alert the CIDC administrators that an upload succeeded."""

    upload_type = (
        upload.assay_type if hasattr(upload, "assay_type") else upload.manifest_type
    )

    subject = f"[UPLOAD SUCCESS]({ENV}) {upload_type} uploaded to {upload.trial_id}"

    html_content = f"""
    <ul>
        <li><strong>upload job id:</strong> {upload.id}</li>
        <li><strong>trial id:</strong> {upload.trial_id}</li>
        <li><strong>type:</strong> {upload_type}</li>
        <li><strong>uploader:</strong> {upload.uploader_email}</li>
    </ul
    """

    email = {
        "to_emails": [CIDC_MAILING_LIST],
        "subject": subject,
        "html_content": html_content,
    }

    return email
