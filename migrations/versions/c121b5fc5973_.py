"""empty message

Revision ID: c121b5fc5973
Revises: 14a807e0e6e8
Create Date: 2020-10-28 10:52:58.987549

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'c121b5fc5973'
down_revision = '14a807e0e6e8'
branch_labels = None
depends_on = None


from cidc_schemas.migrations import v0_21_1_to_v0_22_0

from cidc_api.models.migrations import run_metadata_migration

def upgrade():
    run_metadata_migration(v0_21_1_to_v0_22_0.upgrade, True)


def downgrade():
    run_metadata_migration(v0_21_1_to_v0_22_0.downgrade, True)