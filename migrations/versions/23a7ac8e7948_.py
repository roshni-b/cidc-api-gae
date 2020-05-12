"""empty message

Revision ID: 23a7ac8e7948
Revises: 5f70e6f7c0d9
Create Date: 2019-12-02 12:40:16.956281

"""
from alembic import op
import sqlalchemy as sa

from cidc_api.models.migrations import republish_artifact_uploads

# revision identifiers, used by Alembic.
revision = "23a7ac8e7948"
down_revision = "5f70e6f7c0d9"
branch_labels = None
depends_on = None


def upgrade():
    # Trigger visualization preprocessing
    republish_artifact_uploads()


def downgrade():
    pass
