"""empty message

Revision ID: 571d8a2570a6
Revises: 66897499754f
Create Date: 2019-09-20 09:25:39.672266

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = "571d8a2570a6"
down_revision = "66897499754f"
branch_labels = None
depends_on = None

job_statuses = sa.Enum("started", "completed", "errored", name="job_statuses")

assay_upload_status = sa.Enum(
    "started",
    "upload-completed",
    "upload-failed",
    "merge-completed",
    "merge-failed",
    name="assay_upload_status",
)


def upgrade():
    assay_upload_status.create(op.get_bind())

    op.execute("ALTER TABLE assay_uploads ALTER COLUMN status TYPE text")

    op.execute(
        "UPDATE assay_uploads SET status = 'upload-completed' WHERE status = 'completed'"
    )
    op.execute(
        "UPDATE assay_uploads SET status = 'upload-failed' WHERE status = 'errored'"
    )

    op.execute(
        "ALTER TABLE assay_uploads ALTER COLUMN status TYPE assay_upload_status USING status::assay_upload_status"
    )

    job_statuses.drop(op.get_bind())


def downgrade():
    job_statuses.create(op.get_bind())

    op.execute("ALTER TABLE assay_uploads ALTER COLUMN status TYPE text")
    op.execute(
        "UPDATE assay_uploads SET status = 'completed' WHERE status = 'upload-completed' OR status = 'merge-completed'"
    )
    op.execute(
        "UPDATE assay_uploads SET status = 'errored' WHERE status = 'upload-failed' OR status = 'merge-failed'"
    )
    op.execute(
        "ALTER TABLE assay_uploads ALTER COLUMN status TYPE status::job_statuses"
    )

    assay_upload_status.drop(op.get_bind())
