"""empty message

Revision ID: 1b74e4d0bb7f
Revises: 57cfbda10872
Create Date: 2020-01-23 14:31:00.591414

"""
from alembic import op
import sqlalchemy as sa

from cidc_schemas.migrations import v0_15_2_to_v0_15_3

from cidc_api.models.migrations import run_metadata_migration

# revision identifiers, used by Alembic.
revision = "1b74e4d0bb7f"
down_revision = "57cfbda10872"
branch_labels = None
depends_on = None


def upgrade():
    run_metadata_migration(v0_15_2_to_v0_15_3.upgrade, False)


def downgrade():
    run_metadata_migration(v0_15_2_to_v0_15_3.downgrade, False)
