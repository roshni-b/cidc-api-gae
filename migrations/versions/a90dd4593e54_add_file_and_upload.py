"""add trial_id PK to samples

Revision ID: a90dd4593e54
Revises: 2e81ec9ab77a
Create Date: 2021-06-24 16:02:48.609040

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = 'a90dd4593e54'
down_revision = '2e81ec9ab77a'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('uploads',
    sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
    sa.Column('trial_id', sa.String(), nullable=False),
    sa.Column('status', sa.Enum('started', 'upload-completed', 'upload-failed', 'merge-completed', 'merge-failed', name='upload_status_enum'), server_default='started', nullable=False),
    sa.Column('token', postgresql.UUID(), server_default=sa.text('gen_random_uuid()'), nullable=False),
    sa.Column('status_details', sa.String(), nullable=True),
    sa.Column('multifile', sa.Boolean(), nullable=False),
    sa.Column('gcs_file_map', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
    sa.Column('gcs_xlsx_uri', sa.String(), nullable=True),
    sa.Column('upload_type', sa.String(), nullable=False),
    sa.Column('assay_creator', sa.Enum('DFCI', 'Mount Sinai', 'Stanford', 'MD Anderson', name='artifact_creator_enum'), nullable=False),
    sa.Column('uploader_email', sa.String(), nullable=False),
    sa.ForeignKeyConstraint(['trial_id'], ['clinical_trials.protocol_identifier'], ),
    sa.ForeignKeyConstraint(['uploader_email'], ['users.email'], ),
    sa.PrimaryKeyConstraint('id', 'trial_id')
    )
    op.create_index('upload_gcs_file_map_idx', 'uploads', ['gcs_file_map'], unique=False, postgresql_using='gin')
    op.create_table('files',
    sa.Column('object_url', sa.String(), nullable=False),
    sa.Column('upload_id', sa.Integer(), nullable=False),
    sa.Column('trial_id', sa.String(), nullable=True),
    sa.Column('local_path', sa.String(), nullable=True),
    sa.Column('upload_placeholder', sa.String(), nullable=True),
    sa.Column('artifact_creator', sa.Enum('DFCI', 'Mount Sinai', 'Stanford', 'MD Anderson', name='artifact_creator_enum'), nullable=True),
    sa.Column('uploader', sa.String(), nullable=True),
    sa.Column('file_name', sa.String(), nullable=True),
    sa.Column('uploaded_timestamp', sa.String(), nullable=True),
    sa.Column('md5_hash', sa.String(), nullable=True),
    sa.Column('crc32_hash', sa.String(), nullable=True),
    sa.Column('artifact_category', sa.Enum('Assay Artifact from CIMAC', 'Pipeline Artifact', 'Manifest File', name='artifact_category_enum'), nullable=True),
    sa.Column('data_format', sa.String(), nullable=True),
    sa.Column('facet_group', sa.String(), nullable=True),
    sa.ForeignKeyConstraint(['trial_id', 'upload_id'], ['uploads.trial_id', 'uploads.id'], ),
    sa.PrimaryKeyConstraint('object_url', 'upload_id')
    )
    op.create_table('hande_images',
    sa.Column('object_url', sa.String(), nullable=False),
    sa.Column('upload_id', sa.Integer(), nullable=False),
    sa.ForeignKeyConstraint(['upload_id', 'object_url'], ['files.upload_id', 'files.object_url'], ),
    sa.PrimaryKeyConstraint('object_url')
    )
    op.create_table('hande_records',
    sa.Column('assay_id', sa.Integer(), nullable=False),
    sa.Column('cimac_id', sa.String(), nullable=False),
    sa.Column('trial_id', sa.String(), nullable=False),
    sa.Column('image_url', sa.String(), nullable=False),
    sa.Column('tumor_tissue_percentage', sa.Numeric(), nullable=True),
    sa.Column('viable_tumor_percentage', sa.Numeric(), nullable=True),
    sa.Column('viable_stroma_percentage', sa.Numeric(), nullable=True),
    sa.Column('necrosis_percentage', sa.Numeric(), nullable=True),
    sa.Column('fibrosis_percentage', sa.Numeric(), nullable=True),
    sa.Column('comment', sa.String(), nullable=True),
    sa.ForeignKeyConstraint(['trial_id', 'assay_id'], ['uploads.trial_id', 'uploads.id'], ),
    sa.ForeignKeyConstraint(['trial_id', 'cimac_id'], ['samples.trial_id', 'samples.cimac_id'], ),
    sa.PrimaryKeyConstraint('assay_id', 'cimac_id')
    )
    op.create_unique_constraint('unique_trial_manifest', 'shipments', ['trial_id', 'manifest_id'])
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_constraint('unique_trial_manifest', 'shipments', type_='unique')
    op.drop_table('hande_records')
    op.drop_table('hande_images')
    op.drop_table('files')
    op.drop_index('upload_gcs_file_map_idx', table_name='uploads', postgresql_using='gin')
    op.drop_table('uploads')
    # ### end Alembic commands ###
