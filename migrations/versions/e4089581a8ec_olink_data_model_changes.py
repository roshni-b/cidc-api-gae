"""olink data model changes

Revision ID: e4089581a8ec
Revises: aa2a1eff90cf
Create Date: 2021-03-05 09:37:19.102404

"""
from alembic import op
import sqlalchemy as sa


from cidc_api.models.migrations import run_metadata_migration
from cidc_schemas.migrations import v0_23_18_to_v0_24_0

# revision identifiers, used by Alembic.
revision = "e4089581a8ec"
down_revision = "aa2a1eff90cf"
branch_labels = None
depends_on = None


def upgrade():
    run_metadata_migration(v0_23_18_to_v0_24_0.upgrade, True)


def downgrade():
    # No downgrade - this breaking schema change is not reversible
    pass
