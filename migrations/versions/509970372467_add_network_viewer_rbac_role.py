"""Add network-viewer RBAC role

Revision ID: 509970372467
Revises: e4089581a8ec
Create Date: 2021-03-10 16:06:25.997472

"""
from alembic import op

# revision identifiers, used by Alembic.
revision = "509970372467"
down_revision = "e4089581a8ec"
branch_labels = None
depends_on = None


def upgrade():
    conn = op.get_bind()

    # Recreate the roles enum with 'network-viewer' added as a value
    conn.execute("ALTER TABLE users ALTER role TYPE TEXT")
    conn.execute("DROP TYPE roles")
    conn.execute(
        """CREATE TYPE roles AS ENUM(
            'cidc-admin',
            'cidc-biofx-user',
            'cimac-biofx-user',
            'cimac-user',
            'developer',
            'devops',
            'nci-biobank-user',
            'network-viewer'
        )"""
    )
    conn.execute("ALTER TABLE users ALTER role TYPE roles USING role::roles")


def downgrade():
    conn = op.get_bind()

    # Clear roles for users who are 'network-viewer's
    conn.execute("UPDATE users SET role = null WHERE role = 'network-viewer'")

    # Recreate the roles enum without 'network-viewer' as a value
    conn.execute("ALTER TABLE users ALTER role TYPE TEXT")
    conn.execute("DROP TYPE roles")
    conn.execute(
        """CREATE TYPE roles AS ENUM(
            'cidc-admin',
            'cidc-biofx-user',
            'cimac-biofx-user',
            'cimac-user',
            'developer',
            'devops',
            'nci-biobank-user'
        )"""
    )
    conn.execute("ALTER TABLE users ALTER role TYPE roles USING role::roles")
