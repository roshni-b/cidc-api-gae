from cidc_api.models import Users
from cidc_api.emails import (
    confirm_account_approval,
    new_user_registration,
    CIDC_MAILING_LIST,
)

user = Users(email="foo@bar.com", first_n="john", last_n="doe")


def test_new_user_registration():
    email = new_user_registration(user.email)
    assert user.email in email["html_content"]
    assert email["to_emails"] == [CIDC_MAILING_LIST]
    assert "New User" in email["subject"]
    assert "new user" in email["html_content"]


def test_confirm_account_approval():
    email = confirm_account_approval(user)
    assert user.first_n in email["html_content"]
    assert email["to_emails"] == [user.email]
    assert "Approval" in email["subject"]
    assert "has now been approved" in email["html_content"]
