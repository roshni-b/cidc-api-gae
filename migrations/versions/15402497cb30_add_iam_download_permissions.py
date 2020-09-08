"""add IAM download permissions

Revision ID: 15402497cb30
Revises: 7590a700f9bc
Create Date: 2020-08-28 13:04:44.756182

"""
from alembic import op
import sqlalchemy as sa

from cidc_api.models import Permissions

# revision identifiers, used by Alembic.
revision = "15402497cb30"
down_revision = "7590a700f9bc"
branch_labels = None
depends_on = None


def upgrade():
    session = sa.orm.session.Session(bind=op.get_bind())
    Permissions.grant_all_iam_permissions(session=session)


def downgrade():
    session = sa.orm.session.Session(bind=op.get_bind())
    Permissions.revoke_all_iam_permissions(session=session)
