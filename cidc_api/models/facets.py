from typing import Dict, List, Optional, Union

from werkzeug.exceptions import BadRequest
from sqlalchemy.sql import ClauseElement

Facets = Dict[str, Union[List[str], Dict[str, List[str]]]]

# Represent available downloadable file assay facets as a dictionary
# mapping an assay names to assay subfacet dictionaries. An assay subfacet
# dictionary maps subfacet names to a list of SQLAlchemy filter clause elements
# for looking up files associated with the given subfacet.
assay_facets: Facets = {
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
        "Key": ["%assignment.csv", "%compartment.csv", "%profiling.csv"],
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
}

clinical_facets: Facets = {
    "Participants Info": ["%participants.csv"],
    "Samples Info": ["%samples.csv"],
}

facets = {"Assay Type": assay_facets, "Clinical Type": clinical_facets}


def get_facet_labels():
    return {
        "Assay Type": {
            assay_name: list(subfacets.keys())
            for assay_name, subfacets in assay_facets.items()
        },
        "Clinical Type": list(clinical_facets.keys()),
    }


def get_facets_for_paths(
    object_url_like: ClauseElement, paths: List[List[str]]
) -> List[ClauseElement]:
    clause_args: List[ClauseElement] = []
    for path in paths:
        try:
            assert len(path) in (2, 3)
            path_clauses = facets
            for key in path:
                path_clauses = path_clauses[key]
            assert isinstance(path_clauses, list)
        except Exception as e:
            raise BadRequest(f"no facet for path {path}")
        clause_args.extend(path_clauses)

    clauses = [object_url_like(clause_arg) for clause_arg in clause_args]
    return clauses
