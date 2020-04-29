"""empty message

Revision ID: dc82194b354b
Revises: c60af33175d4
Create Date: 2019-12-05 16:25:35.041783

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "dc82194b354b"
down_revision = "c60af33175d4"
branch_labels = None
depends_on = None


def upgrade():
    op.alter_column(
        table_name="downloadable_files",
        column_name="file_size_bytes",
        type_=sa.BigInteger(),
    )


def downgrade():

    op.alter_column(
        table_name="downloadable_files",
        column_name="file_size_bytes",
        type_=sa.INTEGER(),
    )
