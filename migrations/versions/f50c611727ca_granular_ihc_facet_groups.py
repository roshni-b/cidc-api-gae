"""Granular IHC facet groups

Revision ID: f50c611727ca
Revises: f60b3448eeb8
Create Date: 2020-10-05 16:26:48.796895

"""
from alembic import op
import sqlalchemy as sa

from cidc_api.models import DownloadableFiles, Session


# revision identifiers, used by Alembic.
revision = "f50c611727ca"
down_revision = "f60b3448eeb8"
branch_labels = None
depends_on = None

ihc_facet_group_cases = sa.case(
    [
        (DownloadableFiles.object_url.like("%ihc_image%"), "/ihc/ihc_image."),
        (
            DownloadableFiles.object_url.like("%ihc/combined.csv%"),
            "csv|ihc marker combined",
        ),
    ],
    else_=DownloadableFiles.facet_group,
)


def upgrade():
    session = Session(bind=op.get_bind())
    session.query(DownloadableFiles).update(
        {"facet_group": ihc_facet_group_cases}, synchronize_session="fetch"
    )
    session.commit()


def downgrade():
    session = Session(bind=op.get_bind())
    session.query(DownloadableFiles).filter(
        DownloadableFiles.upload_type.like("ihc%")
    ).update({"facet_group": "Assay Type|IHC|All IHC Files|/ihc"})
    session.commit()
