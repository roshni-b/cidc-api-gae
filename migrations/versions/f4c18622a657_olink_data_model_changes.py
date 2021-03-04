"""olink data model changes

Revision ID: f4c18622a657
Revises: aa2a1eff90cf
Create Date: 2021-03-04 15:34:53.307441

"""
from alembic import op
import sqlalchemy as sa

from cidc_api.models.migrations import run_metadata_migration
from cidc_schemas.migrations import v0_23_18_to_v0_24_0


# revision identifiers, used by Alembic.
revision = "f4c18622a657"
down_revision = "aa2a1eff90cf"
branch_labels = None
depends_on = None


def upgrade():
    run_metadata_migration(v0_23_18_to_v0_24_0.upgrade, True)


def downgrade():
    # No downgrade - this breaking schema change is not reversible
    pass
