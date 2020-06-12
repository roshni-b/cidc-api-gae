"""empty message

Revision ID: 37436458bc78
Revises: e75f457b5b82
Create Date: 2020-04-30 08:38:01.754324

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "37436458bc78"
down_revision = "e75f457b5b82"
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column("users", sa.Column("_accessed", sa.DateTime()))
    # ### end Alembic commands ###

    # Set _accessed to "now" for all existing users (be maximally generous
    # with deactivation time)
    op.execute("UPDATE users SET _accessed = now()")
    op.execute("ALTER TABLE users ALTER COLUMN _accessed SET NOT NULL")


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column("users", "_accessed")
    # ### end Alembic commands ###