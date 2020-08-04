from typing import Dict, List, Optional, Union, NamedTuple, Any

from werkzeug.exceptions import BadRequest
from sqlalchemy.sql import ClauseElement


class FacetConfig:
    match_clauses: List[str]
    description: Optional[str]

    def __init__(self, match_clauses: List[str], description: Optional[str] = None):
        self.match_clauses = match_clauses
        self.description = description


Facets = Dict[str, Union[FacetConfig, Dict[str, FacetConfig]]]

# Represent available downloadable file assay facets as a dictionary
# mapping an assay names to assay subfacet dictionaries. An assay subfacet
# dictionary maps subfacet names to a list of SQLAlchemy filter clause elements
# for looking up files associated with the given subfacet.
assay_facets: Facets = {
    "CyTOF": {
        "Source": FacetConfig(
            [
                "%source_%.fcs",
                "%spike_in_fcs.fcs",
                "%normalized_and_debarcoded.fcs",
                "%processed.fcs",
            ],
            "De-barcoded, concatenated and de-multipled fcs files",
        ),
        "Cell Counts": FacetConfig(
            [
                "%cell_counts_assignment.csv",
                "%cell_counts_compartment.csv",
                "%cell_counts_profiling.csv",
            ],
            "Summary cell count expression of individual cell types in each sample",
        ),
        "Labeled Source": FacetConfig(
            ["%source.fcs"],
            "FCS file with enumerations for compartment, assignment and profiling cell labels",
        ),
        "Analysis Results": FacetConfig(
            ["%analysis.zip", "%results.zip"], "Results package from Astrolabe analysis"
        ),
        "Key": FacetConfig(
            ["%/assignment.csv", "%/compartment.csv", "%/profiling.csv"],
            "Keys for mapping from respective enumeration indices to the cell labels",
        ),
    },
    "WES": {
        "Source": FacetConfig(
            ["%wes%reads_%.bam", "%wes%r1_%.fastq.gz", "%wes%r2_%.fastq.gz"]
        ),
        "Germline": FacetConfig(["%vcfcompare.txt", "%optimalpurityvalue.txt"]),
        "Clonality": FacetConfig(["%clonality_pyclone.tsv"]),
        "Copy Number": FacetConfig(
            ["%copynumber_cnvcalls.txt", "%copynumber_cnvcalls.txt.tn.tsv"]
        ),
        "Neoantigen": FacetConfig(
            [
                "%MHC_Class_I_all_epitopes.tsv",
                "%MHC_Class_I_filtered_condensed_ranked.tsv",
                "%MHC_Class_II_all_epitopes.tsv",
                "%MHC_Class_II_filtered_condensed_ranked.tsv",
            ]
        ),
        "Somatic": FacetConfig(
            [
                "%vcf_tnscope_output.vcf",
                "%maf_tnscope_output.maf",
                "%vcf_tnscope_filter.vcf",
                "%maf_tnscope_filter.maf",
                "%tnscope_exons_broad.gz",
                "%tnscope_exons_mda.gz",
                "%tnscope_exons_mocha.gz",
            ]
        ),
        "Alignment": FacetConfig(
            [
                "%tn_corealigned.bam",
                "%tn_corealigned.bam.bai",
                "%recalibrated.bam",
                "%recalibrated.bam.bai",
                "%sorted.dedup.bam",
                "%sorted.dedup.bam.bai",
            ]
        ),
        "Metrics": FacetConfig(
            [
                "%all_sample_summaries.txt",
                "%coverage_metrics.txt",
                "%target_metrics.txt",
                "%coverage_metrics_summary.txt",
                "%target_metrics_summary.txt",
                "%mosdepth_region_dist_broad.txt",
                "%mosdepth_region_dist_mda.txt",
                "%mosdepth_region_dist_mocha.txt",
                "%optitype_result.tsv",
            ]
        ),
        "HLA Type": FacetConfig(["%optitype_result.tsv"]),
        "Report": FacetConfig(["%wes_version.txt"]),
    },
    "RNA": {
        "Source": FacetConfig(
            ["%rna%reads_%.bam", "%rna%r1_%.fastq.gz", "%rna%r2_%.fastq.gz"]
        ),
        "Alignment": FacetConfig(
            [
                "%sorted.bam",
                "%sorted.bam.bai",
                "%sorted.bam.stat.txt",
                "%downsampling.bam",
                "%downsampling.bam.bai",
            ]
        ),
        "Quality": FacetConfig(
            [
                "%downsampling_housekeeping.bam",
                "%downsampling_housekeeping.bam.bai",
                "%read_distrib.txt",
                "%tin_score.txt",
                "%tin_score.summary.txt",
            ]
        ),
        "Gene Quantification": FacetConfig(
            [
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
            ]
        ),
    },
    "mIF": {
        "Source Images": FacetConfig(
            ["%composite_image.tif", "%component_data.tif", "%multispectral.im3"],
            "Image files containing the source multi-dimensional images for both ROIs and whole slide if appropriate.",
        ),
        "Analysis Images": FacetConfig(
            ["%binary_seg_maps.tif", "%phenotype_map.tif"],
            "Image-like files created or used in the analysis workflow. These include cell and region segmentation maps.",
        ),
        "Analysis Data": FacetConfig(
            ["%score_data_%.txt", "%cell_seg_data.txt", "%cell_seg_data_summary.txt"],
            "Data files from image analysis software indicating the cell type assignments, phenotypes and other scoring metrics and thresholds.",
        ),
    },
    "Olink": {
        "All Olink Files": FacetConfig(
            ["%/olink%"], "Analysis files from the Olink platform."
        )
    },
    "IHC": {"All IHC Files": FacetConfig(["%/ihc%"])},
}

clinical_facets: Facets = {
    "Participants Info": FacetConfig(["%participants.csv"]),
    "Samples Info": FacetConfig(["%samples.csv"]),
}

facets: Dict[str, Facets] = {
    "Assay Type": assay_facets,
    "Clinical Type": clinical_facets,
}


def get_facet_info():
    extract_facet_info = lambda facet_config_list: [
        {"label": label, "description": config.description}
        for label, config in facet_config_list.items()
    ]

    return {
        "Assay Type": {
            assay_name: extract_facet_info(subfacets)
            for assay_name, subfacets in assay_facets.items()
        },
        "Clinical Type": extract_facet_info(clinical_facets),
    }


def get_facets_for_paths(
    object_url_like: ClauseElement, paths: List[List[str]]
) -> List[ClauseElement]:
    clause_list: List[ClauseElement] = []
    for path in paths:
        try:
            assert len(path) in (2, 3)
            facet_config: Any = facets
            for key in path:
                facet_config = facet_config[key]
            assert isinstance(facet_config, FacetConfig)
        except Exception as e:
            raise BadRequest(f"no facet for path {path}")
        clause_list.extend(facet_config.match_clauses)

    clauses = [object_url_like(clause) for clause in clause_list]
    return clauses
