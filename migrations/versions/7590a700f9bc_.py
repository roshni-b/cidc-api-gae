"""empty message

Revision ID: 7590a700f9bc
Revises: e2744508442a
Create Date: 2020-08-19 10:38:55.003131

"""
from alembic import op
import sqlalchemy as sa

from cidc_api.models import DownloadableFiles

# revision identifiers, used by Alembic.
revision = "7590a700f9bc"
down_revision = "e2744508442a"
branch_labels = None
depends_on = None

# NOTE: the below code is based on the contents of cidc_api/models/facets.py at commit hash 48f76f2.
# It's duplicated here so that this migration does not depend on the `cidc_api.models.facets` module.

assay_facets = {
    "CyTOF": {
        "Source": [
            "%source_%.fcs",
            "%spike_in_fcs.fcs",
            "%normalized_and_debarcoded.fcs",
            "%processed.fcs",
        ],
        "Cell Counts": [
            "%cell_counts_assignment.csv",
            "%cell_counts_compartment.csv",
            "%cell_counts_profiling.csv",
        ],
        "Labeled Source": ["%source.fcs"],
        "Analysis Results": ["%analysis.zip", "%results.zip"],
        "Key": ["%/assignment.csv", "%/compartment.csv", "%/profiling.csv"],
    },
    "WES": {
        "Source": ["%wes%reads_%.bam", "%wes%r1_%.fastq.gz", "%wes%r2_%.fastq.gz"],
        "Germline": ["%vcfcompare.txt", "%optimalpurityvalue.txt"],
        "Clonality": ["%clonality_pyclone.tsv"],
        "Copy Number": ["%copynumber_cnvcalls.txt", "%copynumber_cnvcalls.txt.tn.tsv"],
        "Neoantigen": [
            "%MHC_Class_I_all_epitopes.tsv",
            "%MHC_Class_I_filtered_condensed_ranked.tsv",
            "%MHC_Class_II_all_epitopes.tsv",
            "%MHC_Class_II_filtered_condensed_ranked.tsv",
        ],
        "Somatic": [
            "%vcf_tnscope_output.vcf",
            "%maf_tnscope_output.maf",
            "%vcf_tnscope_filter.vcf",
            "%maf_tnscope_filter.maf",
            "%tnscope_exons_broad.gz",
            "%tnscope_exons_mda.gz",
            "%tnscope_exons_mocha.gz",
        ],
        "Alignment": [
            "%tn_corealigned.bam",
            "%tn_corealigned.bam.bai",
            "%recalibrated.bam",
            "%recalibrated.bam.bai",
            "%sorted.dedup.bam",
            "%sorted.dedup.bam.bai",
        ],
        "Metrics": [
            "%all_sample_summaries.txt",
            "%coverage_metrics.txt",
            "%target_metrics.txt",
            "%coverage_metrics_summary.txt",
            "%target_metrics_summary.txt",
            "%mosdepth_region_dist_broad.txt",
            "%mosdepth_region_dist_mda.txt",
            "%mosdepth_region_dist_mocha.txt",
            "%optitype_result.tsv",
        ],
        "HLA Type": ["%optitype_result.tsv"],
        "Report": ["%wes_version.txt"],
    },
    "RNA": {
        "Source": ["%rna%reads_%.bam", "%rna%r1_%.fastq.gz", "%rna%r2_%.fastq.gz"],
        "Alignment": [
            "%sorted.bam",
            "%sorted.bam.bai",
            "%sorted.bam.stat.txt",
            "%downsampling.bam",
            "%downsampling.bam.bai",
        ],
        "Quality": [
            "%downsampling_housekeeping.bam",
            "%downsampling_housekeeping.bam.bai",
            "%read_distrib.txt",
            "%tin_score.txt",
            "%tin_score.summary.txt",
        ],
        "Gene Quantification": [
            "%quant.sf",
            "%transcriptome.bam.log",
            "%aux_info_ambig_info.tsv",
            "%aux_info_expected_bias.gz",
            "%aux_info_fld.gz",
            "%aux_info_meta_info.json",
            "%aux_info_observed_bias.gz",
            "%aux_info_observed_bias_3p.gz",
            "%cmd_info.json",
            "%salmon_quant.log",
        ],
    },
    "mIF": {
        "Source Images": [
            "%composite_image.tif",
            "%component_data.tif",
            "%multispectral.im3",
        ],
        "Analysis Images": ["%binary_seg_maps.tif", "%phenotype_map.tif"],
        "Analysis Data": [
            "%score_data_%.txt",
            "%cell_seg_data.txt",
            "%cell_seg_data_summary.txt",
        ],
    },
    "Olink": {"All Olink Files": ["%/olink%"]},
    "IHC": {"All IHC Files": ["%/ihc%"]},
}

clinical_facets = {
    "Participants Info": ["%participants.csv"],
    "Samples Info": ["%samples.csv"],
}


facets = {"Assay Type": assay_facets, "Clinical Type": clinical_facets}


def list_facet_paths():
    paths = []
    for assay_name, subfacets in assay_facets.items():
        for subfacet in subfacets.keys():
            paths.append(["Assay Type", assay_name, subfacet])

    for clinical_type in clinical_facets.keys():
        paths.append(["Clinical Type", clinical_type])

    return paths


def get_facets_for_path(object_url_like, path):
    try:
        assert len(path) in (2, 3)
        facet_config = facets
        for key in path:
            facet_config = facet_config[key]
        assert isinstance(facet_config, list)
    except Exception as e:
        raise ValueError(f"no facet for path {path}")

    clauses = [object_url_like(clause) for clause in facet_config]
    return clauses


def build_facet_group_for_existing_files():
    """
    Builds a sqlalchemy `case` statement for figuring out which 
    facet group a particular record belongs to.

    This will build facet groups like "Assay Type|mIF|Source Images"
    """
    cases = []
    for path in list_facet_paths():
        path_facets = get_facets_for_path(lambda p: p, path)
        for path_facet in path_facets:
            clean_path_facet = path_facet.replace("%", "")
            facet_group = "|".join([*path, clean_path_facet])
            case_text = sa.text(f"object_url LIKE '{path_facet}'")
            cases.append((case_text, facet_group))

    # Files that don't currently have subfacets will default to [no facet group]
    return sa.case(cases, else_="[no facet group]")


facet_group_cases = build_facet_group_for_existing_files()


def upgrade():
    op.add_column(
        "downloadable_files", sa.Column("facet_group", sa.String(), nullable=True)
    )

    session = sa.orm.session.Session(bind=op.get_bind())
    session.query(DownloadableFiles).update(
        {"facet_group": facet_group_cases}, synchronize_session="fetch"
    )

    op.alter_column(
        "downloadable_files", sa.Column("facet_group", sa.String(), nullable=False)
    )

    session.commit()


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column("downloadable_files", "facet_group")
    # ### end Alembic commands ###
