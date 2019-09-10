"""empty message

Revision ID: d82f2d53d8ba
Revises: 2f75e96dd995
Create Date: 2019-08-29 11:44:04.344630

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'd82f2d53d8ba'
down_revision = '2f75e96dd995'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('assay_uploads', sa.Column('gcs_xlsx_uri', sa.String(), nullable=False))
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('assay_uploads', 'gcs_xlsx_uri')
    # ### end Alembic commands ###
