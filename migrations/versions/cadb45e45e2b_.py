"""empty message

Revision ID: cadb45e45e2b
Revises: 1b74e4d0bb7f
Create Date: 2020-01-24 13:17:57.842983

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "cadb45e45e2b"
down_revision = "1b74e4d0bb7f"
branch_labels = None
depends_on = None


def upgrade():
    conn = op.get_bind()

    conn.execute("ALTER TABLE assay_uploads RENAME TO upload_jobs")
    conn.execute("ALTER TABLE upload_jobs RENAME COLUMN assay_type TO upload_type")
    conn.execute("ALTER TABLE upload_jobs RENAME COLUMN assay_patch TO metadata_patch")
    conn.execute("ALTER TABLE upload_jobs ALTER COLUMN gcs_file_map DROP NOT NULL")
    conn.execute(
        "ALTER TABLE upload_jobs ADD COLUMN multifile BOOLEAN NOT NULL DEFAULT true"
    )
    conn.execute(
        "ALTER TABLE upload_jobs ADD CONSTRAINT check_gcs_file_map CHECK (multifile = false OR gcs_file_map != null)"
    )

    # Transfer data from manifest_uploads to upload_jobs
    conn.execute(
        """
        INSERT INTO upload_jobs(multifile, _created, _updated, _etag, status, status_details, gcs_file_map, gcs_xlsx_uri, metadata_patch, upload_type, uploader_email, trial_id)
        SELECT false, _created, _updated, _etag, 'merge-completed', null, null, gcs_xlsx_uri, metadata_patch, manifest_type, uploader_email, trial_id from manifest_uploads
        """
    )

    op.drop_index("ix_manifest_uploads_trial_id", table_name="manifest_uploads")
    op.drop_table("manifest_uploads")

    conn.execute(
        "ALTER INDEX assay_uploads_gcs_gcs_file_map_idx RENAME TO upload_jobs_gcs_gcs_file_map_idx"
    )
    conn.execute(
        "ALTER INDEX ix_assay_uploads_trial_id RENAME TO ix_upload_jobs_trial_id"
    )
    conn.execute("ALTER TYPE assay_upload_status RENAME TO upload_job_status")


def downgrade():
    op.create_table(
        "manifest_uploads",
        sa.Column(
            "_created", postgresql.TIMESTAMP(), autoincrement=False, nullable=True
        ),
        sa.Column(
            "_updated", postgresql.TIMESTAMP(), autoincrement=False, nullable=True
        ),
        sa.Column("_etag", sa.VARCHAR(length=40), autoincrement=False, nullable=True),
        sa.Column("id", sa.INTEGER(), autoincrement=True, nullable=False),
        sa.Column("gcs_xlsx_uri", sa.VARCHAR(), autoincrement=False, nullable=False),
        sa.Column("uploader_email", sa.VARCHAR(), autoincrement=False, nullable=True),
        sa.Column("trial_id", sa.VARCHAR(), autoincrement=False, nullable=False),
        sa.Column("manifest_type", sa.VARCHAR(), autoincrement=False, nullable=False),
        sa.Column(
            "metadata_patch",
            postgresql.JSONB(astext_type=sa.Text()),
            autoincrement=False,
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["trial_id"],
            ["trial_metadata.trial_id"],
            name="manifest_uploads_trial_id_fkey",
        ),
        sa.ForeignKeyConstraint(
            ["uploader_email"],
            ["users.email"],
            name="manifest_uploads_uploader_email_fkey",
            onupdate="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name="manifest_uploads_pkey"),
    )
    op.create_index(
        "ix_manifest_uploads_trial_id", "manifest_uploads", ["trial_id"], unique=False
    )

    conn = op.get_bind()

    # Transfer data from upload_jobs to manifest_uploads
    conn.execute(
        """
        INSERT INTO manifest_uploads(_created, _updated, _etag, gcs_xlsx_uri, metadata_patch, manifest_type, uploader_email, trial_id)
        SELECT _created, _updated, _etag, gcs_xlsx_uri, metadata_patch, upload_type, uploader_email, trial_id from upload_jobs
        """
    )

    # Drop manifest records
    conn.execute("DELETE FROM upload_jobs WHERE multifile = true")
    conn.execute("ALTER TYPE upload_job_status RENAME TO assay_upload_status")
    conn.execute("ALTER TABLE upload_jobs RENAME TO assay_uploads")
    conn.execute("ALTER TABLE assay_uploads RENAME COLUMN upload_type TO assay_type")
    conn.execute(
        "ALTER TABLE assay_uploads RENAME COLUMN metadata_patch TO assay_patch"
    )
    conn.execute("ALTER TABLE assay_uploads ALTER COLUMN gcs_file_map SET NOT NULL")
    conn.execute("ALTER TABLE assay_uploads DROP COLUMN multifile")
    conn.execute("ALTER TABLE assay_uploads DROP CONSTRAINT check_gcs_file_map")

    op.drop_index("ix_manifest_uploads_trial_id", table_name="manifest_uploads")
    op.drop_table("manifest_uploads")

    conn.execute(
        "ALTER INDEX upload_jobs_gcs_gcs_file_map_idx RENAME TO assay_uploads_gcs_gcs_file_map_idx"
    )
    conn.execute(
        "ALTER INDEX ix_upload_jobs_trial_id RENAME TO ix_assay_uploads_trial_id"
    )
    conn.execute("ALTER TYPE upload_job_status RENAME TO assay_upload_status")
