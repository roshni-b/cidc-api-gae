"""Move arbitrary_trial_specific_clinical_annotations to clinical

Revision ID: f7e888b7be33
Revises: c121b5fc5973
Create Date: 2020-12-01 12:42:33.126968

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'f7e888b7be33'
down_revision = 'c121b5fc5973'
branch_labels = None
depends_on = None

from cidc_schemas.migrations import v0_23_0_to_v0_23_1

from cidc_api.models.migrations import run_metadata_migration

def upgrade():
    run_metadata_migration(v0_23_0_to_v0_23_1.upgrade, True)


def downgrade():
    run_metadata_migration(v0_23_0_to_v0_23_1.downgrade, True)
