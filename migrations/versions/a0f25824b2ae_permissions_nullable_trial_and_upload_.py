"""permissions nullable trial and upload type

Revision ID: a0f25824b2ae
Revises: 7d3ad965db30
Create Date: 2021-04-20 14:57:14.708607

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = "a0f25824b2ae"
down_revision = "7d3ad965db30"
branch_labels = None
depends_on = None


def upgrade():
    op.alter_column(
        "permissions", "trial_id", existing_type=sa.VARCHAR(), nullable=True
    )
    op.alter_column(
        "permissions", "upload_type", existing_type=sa.VARCHAR(), nullable=True
    )

    # By default, Postgres allows multiple NULL values in its unique indexes,
    # so we need to introduce additional uniqueness constraints that disallow
    # multiple permissions with NULL trial_id's or upload_type's.
    op.execute(
        """
            CREATE UNIQUE INDEX 
                unique_trial_id_upload_type_is_null_perms
            ON 
                permissions (granted_to_user, trial_id, (upload_type IS NULL)) 
            WHERE 
                upload_type IS NULL
        """
    )
    op.execute(
        """
            CREATE UNIQUE INDEX 
                unique_upload_type_trial_id_is_null_perms
            ON 
                permissions (granted_to_user, (trial_id IS NULL), upload_type) 
            WHERE 
                trial_id IS NULL
        """
    )

    op.create_check_constraint(
        "ck_nonnull_trial_id_or_upload_type",
        "permissions",
        "trial_id is not null or upload_type is not null",
    )


def downgrade():
    op.execute("DROP INDEX unique_trial_id_upload_type_is_null_perms")
    op.execute("DROP INDEX unique_upload_type_trial_id_is_null_perms")

    # NOTE: it is up to the developer making the decision to downgrade
    # to delete the corresponding GCS IAM permissions via some other route.
    # I don't think we want database migrations performing IAM actions.
    op.execute("DELETE FROM permissions WHERE upload_type IS NULL OR trial_id IS NULL")

    op.drop_constraint("ck_nonnull_trial_id_or_upload_type", "permissions")
    op.alter_column(
        "permissions", "upload_type", existing_type=sa.VARCHAR(), nullable=False
    )
    op.alter_column(
        "permissions", "trial_id", existing_type=sa.VARCHAR(), nullable=False
    )
