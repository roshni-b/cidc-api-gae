"""Granular Olink facet groups

Revision ID: f60b3448eeb8
Revises: 26ba8b4e9b51
Create Date: 2020-09-28 08:35:51.265610

"""
from alembic import op
import sqlalchemy as sa

from cidc_api.models import Session, DownloadableFiles

# revision identifiers, used by Alembic.
revision = "f60b3448eeb8"
down_revision = "26ba8b4e9b51"
branch_labels = None
depends_on = None

olink_facet_group_cases = sa.case(
    [
        (
            DownloadableFiles.object_url.like("%olink%assay_npx.xlsx"),
            "/olink/chip_/assay_npx.xlsx",
        ),
        (
            DownloadableFiles.object_url.like("%olink%assay_raw_ct.csv"),
            "/olink/chip_/assay_raw_ct.csv",
        ),
        (
            DownloadableFiles.object_url.like("%olink%study_npx.xlsx"),
            "/olink/study_npx.xlsx",
        ),
    ],
    else_=DownloadableFiles.facet_group,
)


def upgrade():
    session = Session(bind=op.get_bind())
    session.query(DownloadableFiles).update(
        {"facet_group": olink_facet_group_cases}, synchronize_session="fetch"
    )
    session.commit()


def downgrade():
    session = Session(bind=op.get_bind())
    session.query(DownloadableFiles).filter(
        DownloadableFiles.upload_type == "olink"
    ).update({"facet_group": "Assay Type|Olink|All Olink Files|/olink"})
    session.commit()
