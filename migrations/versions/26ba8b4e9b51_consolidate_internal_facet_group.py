"""Consolidate internal facet group

Revision ID: 26ba8b4e9b51
Revises: 7590a700f9bc
Create Date: 2020-09-16 11:11:07.214451

"""
from alembic import op
import sqlalchemy as sa

from cidc_api.models import Session, DownloadableFiles


# revision identifiers, used by Alembic.
revision = "26ba8b4e9b51"
down_revision = "7590a700f9bc"
branch_labels = None
depends_on = None

renaming_map = {
    "Assay Type|CyTOF|Analysis Results|analysis.zip": "/cytof_analysis/reports.zip",
    "Assay Type|CyTOF|Analysis Results|results.zip": "/cytof_analysis/analysis.zip",
    "Assay Type|CyTOF|Cell Counts|cell_counts_assignment.csv": "/cytof_analysis/cell_counts_assignment.csv",
    "Assay Type|CyTOF|Cell Counts|cell_counts_compartment.csv": "/cytof_analysis/cell_counts_compartment.csv",
    "Assay Type|CyTOF|Cell Counts|cell_counts_profiling.csv": "/cytof_analysis/cell_counts_profiling.csv",
    "Assay Type|CyTOF|Key|/assignment.csv": "/cytof_analysis/assignment.csv",
    "Assay Type|CyTOF|Key|/compartment.csv": "/cytof_analysis/compartment.csv",
    "Assay Type|CyTOF|Key|/profiling.csv": "/cytof_analysis/profiling.csv",
    "Assay Type|CyTOF|Labeled Source|source.fcs": "/cytof_analysis/source.fcs",
    "Assay Type|CyTOF|Source|normalized_and_debarcoded.fcs": "/cytof/normalized_and_debarcoded.fcs",
    "Assay Type|CyTOF|Source|processed.fcs": "/cytof/processed.fcs",
    "Assay Type|CyTOF|Source|source_.fcs": "/cytof/source_.fcs",
    "Assay Type|CyTOF|Source|spike_in.fcs": "/cytof/spike_in.fcs",
    "Assay Type|RNA|Alignment|downsampling.bam": "/rna/analysis/star/downsampling.bam",
    "Assay Type|RNA|Alignment|downsampling.bam.bai": "/rna/analysis/star/downsampling.bam.bai",
    "Assay Type|RNA|Alignment|sorted.bam": "/rna/analysis/star/sorted.bam",
    "Assay Type|RNA|Alignment|sorted.bam.bai": "/rna/analysis/star/sorted.bam.bai",
    "Assay Type|RNA|Alignment|sorted.bam.stat.txt": "/rna/analysis/star/sorted.bam.stat.txt",
    "Assay Type|RNA|Gene Quantification|aux_info_ambig_info.tsv": "/rna/analysis/salmon/aux_info_ambig_info.tsv",
    "Assay Type|RNA|Gene Quantification|aux_info_expected_bias.gz": "/rna/analysis/salmon/aux_info_expected_bias.gz",
    "Assay Type|RNA|Gene Quantification|aux_info_fld.gz": "/rna/analysis/salmon/aux_info_fld.gz",
    "Assay Type|RNA|Gene Quantification|aux_info_meta_info.json": "/rna/analysis/salmon/aux_info_meta_info.json",
    "Assay Type|RNA|Gene Quantification|aux_info_observed_bias.gz": "/rna/analysis/salmon/aux_info_observed_bias.gz",
    "Assay Type|RNA|Gene Quantification|aux_info_observed_bias_3p.gz": "/rna/analysis/salmon/aux_info_observed_bias_3p.gz",
    "Assay Type|RNA|Gene Quantification|cmd_info.json": "/rna/analysis/salmon/cmd_info.json",
    "Assay Type|RNA|Gene Quantification|quant.sf": "/rna/analysis/salmon/quant.sf",
    "Assay Type|RNA|Gene Quantification|salmon_quant.log": "/rna/analysis/salmon/salmon_quant.log",
    "Assay Type|RNA|Gene Quantification|transcriptome.bam.log": "/rna/analysis/salmon/transcriptome.bam.log",
    "Assay Type|RNA|Quality|downsampling_housekeeping.bam": "/rna/analysis/rseqc/downsampling_housekeeping.bam",
    "Assay Type|RNA|Quality|downsampling_housekeeping.bam.bai": "/rna/analysis/rseqc/downsampling_housekeeping.bam.bai",
    "Assay Type|RNA|Quality|read_distrib.txt": "/rna/analysis/rseqc/read_distrib.txt",
    "Assay Type|RNA|Quality|tin_score.summary.txt": "/rna/analysis/rseqc/tin_score.summary.txt",
    "Assay Type|RNA|Quality|tin_score.txt": "/rna/analysis/rseqc/tin_score.txt",
    "Assay Type|RNA|Source|rnar1_.fastq.gz": "/rna/r1_.fastq.gz",
    "Assay Type|RNA|Source|rnar2_.fastq.gz": "/rna/r2_.fastq.gz",
    "Assay Type|RNA|Source|rnareads_.bam": "/rna/reads_.bam",
    "Assay Type|WES|Alignment|tn_corealigned.bam": "/wes/analysis/tn_corealigned.bam",
    "Assay Type|WES|Alignment|tn_corealigned.bam.bai": "/wes/analysis/tn_corealigned.bam.bai",
    "Assay Type|WES|Clonality|clonality_pyclone.tsv": "/wes/analysis/clonality_pyclone.tsv",
    "Assay Type|WES|Copy Number|copynumber_cnvcalls.txt": "/wes/analysis/copynumber_cnvcalls.txt",
    "Assay Type|WES|Copy Number|copynumber_cnvcalls.txt.tn.tsv": "/wes/analysis/copynumber_cnvcalls.txt.tn.tsv",
    "Assay Type|WES|Germline|optimalpurityvalue.txt": "/wes/analysis/optimalpurityvalue.txt",
    "Assay Type|WES|Germline|vcfcompare.txt": "/wes/analysis/vcfcompare.txt",
    "Assay Type|WES|Neoantigen|MHC_Class_II_all_epitopes.tsv": "/wes/analysis/MHC_Class_II_all_epitopes.tsv",
    "Assay Type|WES|Neoantigen|MHC_Class_II_filtered_condensed_ranked.tsv": "/wes/analysis/MHC_Class_II_filtered_condensed_ranked.tsv",
    "Assay Type|WES|Neoantigen|MHC_Class_I_all_epitopes.tsv": "/wes/analysis/MHC_Class_I_all_epitopes.tsv",
    "Assay Type|WES|Neoantigen|MHC_Class_I_filtered_condensed_ranked.tsv": "/wes/analysis/MHC_Class_I_filtered_condensed_ranked.tsv",
    "Assay Type|WES|Report|wes_version.txt": "/wes/analysis/wes_version.txt",
    "Assay Type|WES|Somatic|maf_tnscope_filter.maf": "/wes/analysis/maf_tnscope_filter.maf",
    "Assay Type|WES|Somatic|maf_tnscope_output.maf": "/wes/analysis/maf_tnscope_output.maf",
    "Assay Type|WES|Somatic|tnscope_exons_broad.gz": "/wes/analysis/tnscope_exons_broad.gz",
    "Assay Type|WES|Somatic|tnscope_exons_mda.gz": "/wes/analysis/tnscope_exons_mda.gz",
    "Assay Type|WES|Somatic|tnscope_exons_mocha.gz": "/wes/analysis/tnscope_exons_mocha.gz",
    "Assay Type|WES|Somatic|vcf_tnscope_filter.vcf": "/wes/analysis/vcf_tnscope_filter.vcf",
    "Assay Type|WES|Somatic|vcf_tnscope_output.vcf": "/wes/analysis/vcf_tnscope_output.vcf",
    "Assay Type|WES|Source|wesr1_.fastq.gz": "/wes/r1_.fastq.gz",
    "Assay Type|WES|Source|wesr2_.fastq.gz": "/wes/r2_.fastq.gz",
    "Assay Type|WES|Source|wesreads_.bam": "/wes/reads_.bam",
    "Assay Type|mIF|Analysis Data|cell_seg_data.txt": "/mif/roi_/cell_seg_data.txt",
    "Assay Type|mIF|Analysis Data|cell_seg_data_summary.txt": "/mif/roi_/cell_seg_data_summary.txt",
    "Assay Type|mIF|Analysis Data|score_data_.txt": "/mif/roi_/score_data_.txt",
    "Assay Type|mIF|Analysis Images|binary_seg_maps.tif": "/mif/roi_/binary_seg_maps.tif",
    "Assay Type|mIF|Analysis Images|phenotype_map.tif": "/mif/roi_/phenotype_map.tif",
    "Assay Type|mIF|Source Images|component_data.tif": "/mif/roi_/component_data.tif",
    "Assay Type|mIF|Source Images|composite_image.tif": "/mif/roi_/composite_image.tif",
    "Assay Type|mIF|Source Images|multispectral.im3": "/mif/roi_/multispectral.im3",
    "Assay Type|WES|Metrics|all_sample_summaries.txt": "/wes/analysis/metrics/all_sample_summaries.txt",
    "Assay Type|WES|Alignment|recalibrated.bam": {
        "tumor": "/wes/analysis/tumor/recalibrated.bam",
        "normal": "/wes/analysis/normal/recalibrated.bam",
    },
    "Assay Type|WES|Alignment|recalibrated.bam.bai": {
        "tumor": "/wes/analysis/tumor/recalibrated.bam.bai",
        "normal": "/wes/analysis/normal/recalibrated.bam.bai",
    },
    "Assay Type|WES|Alignment|sorted.dedup.bam": {
        "tumor": "/wes/analysis/tumor/sorted.dedup.bam",
        "normal": "/wes/analysis/normal/sorted.dedup.bam",
    },
    "Assay Type|WES|Alignment|sorted.dedup.bam.bai": {
        "tumor": "/wes/analysis/tumor/sorted.dedup.bam.bai",
        "normal": "/wes/analysis/normal/sorted.dedup.bam.bai",
    },
    "Assay Type|WES|Metrics|coverage_metrics.txt": {
        "tumor": "/wes/analysis/tumor/coverage_metrics.txt",
        "normal": "/wes/analysis/normal/coverage_metrics.txt",
    },
    "Assay Type|WES|Metrics|coverage_metrics_summary.txt": {
        "tumor": "/wes/analysis/tumor/coverage_metrics_summary.txt",
        "normal": "/wes/analysis/normal/coverage_metrics_summary.txt",
    },
    "Assay Type|WES|Metrics|mosdepth_region_dist_broad.txt": {
        "tumor": "/wes/analysis/tumor/mosdepth_region_dist_broad.txt",
        "normal": "/wes/analysis/normal/mosdepth_region_dist_broad.txt",
    },
    "Assay Type|WES|Metrics|mosdepth_region_dist_mda.txt": {
        "tumor": "/wes/analysis/tumor/mosdepth_region_dist_mda.txt",
        "normal": "/wes/analysis/normal/mosdepth_region_dist_mda.txt",
    },
    "Assay Type|WES|Metrics|mosdepth_region_dist_mocha.txt": {
        "tumor": "/wes/analysis/tumor/mosdepth_region_dist_mocha.txt",
        "normal": "/wes/analysis/normal/mosdepth_region_dist_mocha.txt",
    },
    "Assay Type|WES|Metrics|target_metrics.txt": {
        "tumor": "/wes/analysis/tumor/target_metrics.txt",
        "normal": "/wes/analysis/normal/target_metrics.txt",
    },
    "Assay Type|WES|Metrics|target_metrics_summary.txt": {
        "tumor": "/wes/analysis/tumor/target_metrics_summary.txt",
        "normal": "/wes/analysis/normal/target_metrics_summary.txt",
    },
    "Assay Type|WES|HLA Type|optitype_result.tsv": {
        "tumor": "/wes/analysis/tumor/optitype_result.tsv",
        "normal": "/wes/analysis/normal/optitype_result.tsv",
    },
    "Clinical Type|Participants Info|participants.csv": "csv|participants info",
    "Clinical Type|Samples Info|samples.csv": "csv|samples info",
}


def build_facet_group_name_change():
    cases = []
    for old_group, new_group in renaming_map.items():
        if isinstance(new_group, str):
            cases.append((DownloadableFiles.facet_group == old_group, new_group))
        elif isinstance(new_group, dict):
            # Old facet groups don't distinguish between tumor and normal,
            # but new facet groups do.
            assert "tumor" in new_group and "normal" in new_group
            case_tumor_query = sa.and_(
                DownloadableFiles.facet_group == old_group,
                DownloadableFiles.object_url.like("%tumor%"),
            )
            case_normal_query = sa.and_(
                DownloadableFiles.facet_group == old_group,
                DownloadableFiles.object_url.like("%normal%"),
            )
            cases.extend(
                [
                    (case_normal_query, new_group["normal"]),
                    (case_tumor_query, new_group["tumor"]),
                ]
            )
        else:
            raise Exception("whoops! migration misconfigured")

    # Some facet groups don't need to be renamed, so default to
    # not changing a file's facet_group if the facet_group doesn't
    # show up in the renaming map.
    return sa.case(cases, else_=DownloadableFiles.facet_group)


facet_group_updates = build_facet_group_name_change()


def upgrade():
    session = Session(bind=op.get_bind())
    session.query(DownloadableFiles).update(
        {"facet_group": facet_group_updates}, synchronize_session="fetch"
    )
    session.commit()


def downgrade():
    # NOTE: this operation is not reversible
    pass
