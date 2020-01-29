"""empty message

Revision ID: 2ef21fa29d1b
Revises: cadb45e45e2b
Create Date: 2020-01-24 15:05:09.997600

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "2ef21fa29d1b"
down_revision = "cadb45e45e2b"
branch_labels = None
depends_on = None


def upgrade():
    conn = op.get_bind()
    conn.execute(
        "ALTER TABLE downloadable_files RENAME COLUMN assay_type TO upload_type"
    )
    conn.execute("ALTER TABLE permissions RENAME COLUMN assay_type TO upload_type")


def downgrade():
    conn = op.get_bind()
    conn.execute(
        "ALTER TABLE downloadable_files RENAME COLUMN upload_type TO assay_type"
    )
    conn.execute("ALTER TABLE permissions RENAME COLUMN upload_type TO assay_type")
