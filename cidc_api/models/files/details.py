from typing import NamedTuple, Optional
from typing_extensions import Literal


FilePurpose = Literal["source", "analysis", "clinical", "miscellaneous"]


class FileDetails(NamedTuple):
    file_purpose: FilePurpose
    # long term, all files should have descriptions
    short_description: Optional[str] = None
    long_description: Optional[str] = None


details_dict = {
    # CyTOF
    "/cytof_analysis/source.fcs": FileDetails("source"),
    "/cytof/spike_in.fcs": FileDetails("source"),
    "/cytof/source_.fcs": FileDetails("source"),
    "/cytof/normalized_and_debarcoded.fcs": FileDetails("source"),
    "/cytof/processed.fcs": FileDetails("source"),
    "/cytof_analysis/cell_counts_assignment.csv": FileDetails("analysis"),
    "/cytof_analysis/cell_counts_compartment.csv": FileDetails("analysis"),
    "/cytof_analysis/cell_counts_profiling.csv": FileDetails("analysis"),
    "/cytof_analysis/assignment.csv": FileDetails("analysis"),
    "/cytof_analysis/compartment.csv": FileDetails("analysis"),
    "/cytof_analysis/profiling.csv": FileDetails("analysis"),
    # WES
    "/wes/r1_.fastq.gz": FileDetails("source"),
    "/wes/r2_.fastq.gz": FileDetails("source"),
    "/wes/reads_.bam": FileDetails("source"),
    ## see: https://github.com/CIMAC-CIDC/cidc-ngs-pipeline-api/blob/master/wes/wes_output_API.json
    "/wes/analysis/vcfcompare.txt": FileDetails("miscellaneous"),
    "/wes/analysis/optimalpurityvalue.txt": FileDetails("miscellaneous"),
    "/wes/analysis/clonality_pyclone.tsv": FileDetails("miscellaneous"),
    "/wes/analysis/copynumber_cnvcalls.txt": FileDetails("miscellaneous"),
    "/wes/analysis/copynumber_cnvcalls.txt.tn.tsv": FileDetails("miscellaneous"),
    "/wes/analysis/MHC_Class_I_all_epitopes.tsv": FileDetails("miscellaneous"),
    "/wes/analysis/MHC_Class_I_filtered_condensed_ranked.tsv": FileDetails(
        "miscellaneous"
    ),
    "/wes/analysis/MHC_Class_II_all_epitopes.tsv": FileDetails("miscellaneous"),
    "/wes/analysis/MHC_Class_II_filtered_condensed_ranked.tsv": FileDetails(
        "miscellaneous"
    ),
    "/wes/analysis/vcf_tnscope_output.vcf": FileDetails("miscellaneous"),
    "/wes/analysis/maf_tnscope_output.maf": FileDetails("miscellaneous"),
    "/wes/analysis/vcf_tnscope_filter.vcf": FileDetails("miscellaneous"),
    "/wes/analysis/maf_tnscope_filter.maf": FileDetails("miscellaneous"),
    "/wes/analysis/tnscope_exons_broad.gz": FileDetails("miscellaneous"),
    "/wes/analysis/tnscope_exons_mda.gz": FileDetails("miscellaneous"),
    "/wes/analysis/tnscope_exons_mocha.gz": FileDetails("miscellaneous"),
    "/wes/analysis/tn_corealigned.bam": FileDetails("miscellaneous"),
    "/wes/analysis/tn_corealigned.bam.bai": FileDetails("miscellaneous"),
    "/wes/analysis/tumor/recalibrated.bam": FileDetails("miscellaneous"),
    "/wes/analysis/tumor/recalibrated.bam.bai": FileDetails("miscellaneous"),
    "/wes/analysis/tumor/sorted.dedup.bam": FileDetails("miscellaneous"),
    "/wes/analysis/tumor/sorted.dedup.bam.bai": FileDetails("miscellaneous"),
    "/wes/analysis/normal/recalibrated.bam": FileDetails("miscellaneous"),
    "/wes/analysis/normal/recalibrated.bam.bai": FileDetails("miscellaneous"),
    "/wes/analysis/normal/sorted.dedup.bam": FileDetails("miscellaneous"),
    "/wes/analysis/normal/sorted.dedup.bam.bai": FileDetails("miscellaneous"),
    "/wes/analysis/tumor/coverage_metrics.txt": FileDetails("miscellaneous"),
    "/wes/analysis/tumor/target_metrics.txt": FileDetails("miscellaneous"),
    "/wes/analysis/tumor/coverage_metrics_summary.txt": FileDetails("miscellaneous"),
    "/wes/analysis/tumor/target_metrics_summary.txt": FileDetails("miscellaneous"),
    "/wes/analysis/tumor/mosdepth_region_dist_broad.txt": FileDetails("miscellaneous"),
    "/wes/analysis/tumor/mosdepth_region_dist_mda.txt": FileDetails("miscellaneous"),
    "/wes/analysis/tumor/mosdepth_region_dist_mocha.txt": FileDetails("miscellaneous"),
    "/wes/analysis/normal/coverage_metrics.txt": FileDetails("miscellaneous"),
    "/wes/analysis/normal/target_metrics.txt": FileDetails("miscellaneous"),
    "/wes/analysis/normal/coverage_metrics_summary.txt": FileDetails("miscellaneous"),
    "/wes/analysis/normal/target_metrics_summary.txt": FileDetails("miscellaneous"),
    "/wes/analysis/normal/mosdepth_region_dist_broad.txt": FileDetails("miscellaneous"),
    "/wes/analysis/normal/mosdepth_region_dist_mda.txt": FileDetails("miscellaneous"),
    "/wes/analysis/normal/mosdepth_region_dist_mocha.txt": FileDetails("miscellaneous"),
    "/wes/analysis/tumor/optitype_result.tsv": FileDetails("miscellaneous"),
    "/wes/analysis/normal/optitype_result.tsv": FileDetails("miscellaneous"),
    "/wes/analysis/wes_version.txt": FileDetails("miscellaneous"),
    # RNA
    "/rna/r1_.fastq.gz": FileDetails("source"),
    "/rna/r2_.fastq.gz": FileDetails("source"),
    "/rna/reads_.bam": FileDetails("source"),
    ## see: https://github.com/CIMAC-CIDC/cidc-ngs-pipeline-api/blob/master/rna/rna_level1_output_API.json
    "/rna/analysis/star/sorted.bam": FileDetails("analysis"),
    "/rna/analysis/star/sorted.bam.bai": FileDetails("miscellaneous"),
    "/rna/analysis/star/sorted.bam.stat.txt": FileDetails("miscellaneous"),
    "/rna/analysis/star/downsampling.bam": FileDetails("miscellaneous"),
    "/rna/analysis/star/downsampling.bam.bai": FileDetails("miscellaneous"),
    "/rna/analysis/rseqc/downsampling_housekeeping.bam": FileDetails("miscellaneous"),
    "/rna/analysis/rseqc/downsampling_housekeeping.bam.bai": FileDetails(
        "miscellaneous"
    ),
    "/rna/analysis/rseqc/read_distrib.txt": FileDetails("clinical"),
    "/rna/analysis/rseqc/tin_score.summary.txt": FileDetails("miscellaneous"),
    "/rna/analysis/rseqc/tin_score.txt": FileDetails("analysis"),
    "/rna/analysis/salmon/quant.sf": FileDetails("miscellaneous"),
    "/rna/analysis/salmon/transcriptome.bam.log": FileDetails("miscellaneous"),
    "/rna/analysis/salmon/aux_info_ambig_info.tsv": FileDetails("miscellaneous"),
    "/rna/analysis/salmon/aux_info_expected_bias.gz": FileDetails("miscellaneous"),
    "/rna/analysis/salmon/aux_info_meta_info.json": FileDetails("miscellaneous"),
    "/rna/analysis/salmon/aux_info_observed_bias.gz": FileDetails("miscellaneous"),
    "/rna/analysis/salmon/aux_info_observed_bias_3p.gz": FileDetails("miscellaneous"),
    "/rna/analysis/salmon/cmd_info.json": FileDetails("miscellaneous"),
    "/rna/analysis/salmon/salmon_quant.log": FileDetails("miscellaneous"),
    # mIF
    "/mif/roi_/composite_image.tif": FileDetails("source"),
    "/mif/roi_/component_data.tif": FileDetails("source"),
    "/mif/roi_/multispectral.im3": FileDetails("source"),
    "/mif/roi_/binary_seg_maps.tif": FileDetails("analysis"),
    "/mif/roi_/phenotype_map.tif": FileDetails("analysis"),
    "/mif/roi_/score_data_.txt": FileDetails("analysis"),
    "/mif/roi_/cell_seg_data.txt": FileDetails("analysis"),
    "/mif/roi_/cell_seg_data_summary.txt": FileDetails("analysis"),
    # Olink
    "Assay Type|Olink|All Olink Files|/olink": FileDetails("miscellaneous"),
    "/olink/study_npx.xlsx": FileDetails("analysis"),
    "/olink/chip_/assay_npx.xlsx": FileDetails("analysis"),
    "/olink/chip_/assay_raw_ct.csv": FileDetails("source"),
    # IHC
    "/ihc/ihc_image.": FileDetails("source"),
    "csv|ihc marker combined": FileDetails("analysis"),
    # Clinical
    "csv|participants info": FileDetails("clinical"),
    "csv|samples info": FileDetails("clinical"),
    # TCR
    "/tcr/replicate_/r1.fastq.gz": FileDetails("source"),
    "/tcr/replicate_/r2.fastq.gz": FileDetails("source"),
    "/tcr/replicate_/i1.fastq.gz": FileDetails("source"),
    "/tcr/replicate_/i2.fastq.gz": FileDetails("source"),
    "/tcr/SampleSheet.csv": FileDetails("miscellaneous"),
    "/tcr/summary_info.csv": FileDetails("miscellaneous"),
    "/tcr/tra_clone.csv": FileDetails("analysis"),
    "/tcr/trb_clone.csv": FileDetails("analysis"),
}
