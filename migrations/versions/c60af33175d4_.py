"""empty message

Revision ID: c60af33175d4
Revises: 23a7ac8e7948
Create Date: 2019-12-03 11:32:17.224586

"""
from cidc_api.models.migrations import republish_artifact_uploads

# revision identifiers, used by Alembic.
revision = "c60af33175d4"
down_revision = "23a7ac8e7948"
branch_labels = None
depends_on = None


def upgrade():
    # Integrate in changes from https://github.com/CIMAC-CIDC/cidc-cloud-functions/pull/80
    republish_artifact_uploads()


def downgrade():
    pass
