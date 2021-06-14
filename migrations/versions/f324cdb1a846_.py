"""Update downloadable_files data_format and assay_type columns to CITEXT.

Revision ID: f324cdb1a846
Revises: 0c924d67603c
Create Date: 2019-12-20 10:02:11.055006

"""
from alembic import op

# revision identifiers, used by Alembic.
revision = "f324cdb1a846"
down_revision = "0c924d67603c"
branch_labels = None
depends_on = None


def upgrade():
    # NOTE: this requires that a super user has already run `CREATE EXTENSION citext` on the db
    op.execute("ALTER TABLE downloadable_files ALTER COLUMN data_format TYPE CITEXT")
    op.execute("ALTER TABLE downloadable_files ALTER COLUMN assay_type TYPE CITEXT")


def downgrade():
    op.execute("ALTER TABLE downloadable_files ALTER COLUMN data_format TYPE VARCHAR")
    op.execute("ALTER TABLE downloadable_files ALTER COLUMN assay_type TYPE VARCHAR")
