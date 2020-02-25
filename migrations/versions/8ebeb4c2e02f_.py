"""empty message

Revision ID: 8ebeb4c2e02f
Revises: 2ef21fa29d1b
Create Date: 2020-01-30 15:43:59.227314

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "8ebeb4c2e02f"
down_revision = "2ef21fa29d1b"
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column(
        "downloadable_files",
        sa.Column("analysis_friendly", sa.Boolean(), nullable=True),
    )
    conn = op.get_bind()
    conn.execute(
        """
        UPDATE downloadable_files SET analysis_friendly = true 
        WHERE upload_type in ('participants info', 'samples info', 'cell counts assignment', 'cell counts compartment', 'cell counts profiling', 'combined maf', 'ihc marker combined')
        """
    )
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column("downloadable_files", "analysis_friendly")
    # ### end Alembic commands ###