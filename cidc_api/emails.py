"""Template functions for CIDC email bodies."""

from models import Users
from config.settings import ENV

CIDC_MAILING_LIST = "cidc@jimmy.harvard.edu"


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
