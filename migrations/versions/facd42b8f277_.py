"""empty message

Revision ID: facd42b8f277
Revises: ff3141aecdd4
Create Date: 2019-11-25 09:34:02.252937

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from cidc_schemas.migrations import v0_10_2_to_v0_11_0
from cidc_api.utils.migrations import run_metadata_migration


# revision identifiers, used by Alembic.
revision = 'facd42b8f277'
down_revision = 'ff3141aecdd4'
branch_labels = None
depends_on = None


def upgrade():
    """Update Olink's assay_raw_ct artifact data format to CSV"""
    run_metadata_migration(v0_10_2_to_v0_11_0.upgrade)


def downgrade():
    """Downgrade Olink's assay_raw_ct artifact data format to XLSX"""
    run_metadata_migration(v0_10_2_to_v0_11_0.downgrade)
