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
                "/cytof/spike_in.fcs",
                "/cytof/source_.fcs",
                "/cytof/normalized_and_debarcoded.fcs",
                "/cytof/processed.fcs",
            ],
            "De-barcoded, concatenated and de-multipled fcs files",
        ),
        "Cell Counts": FacetConfig(
            [
                "/cytof_analysis/cell_counts_assignment.csv",
                "/cytof_analysis/cell_counts_compartment.csv",
                "/cytof_analysis/cell_counts_profiling.csv",
            ],
            "Summary cell count expression of individual cell types in each sample",
        ),
        "Labeled Source": FacetConfig(
            ["/cytof_analysis/source.fcs"],
            "FCS file with enumerations for compartment, assignment and profiling cell labels",
        ),
        "Analysis Results": FacetConfig(
            ["/cytof_analysis/reports.zip", "/cytof_analysis/analysis.zip"],
            "Results package from Astrolabe analysis",
        ),
        "Key": FacetConfig(
            [
                "/cytof_analysis/assignment.csv",
                "/cytof_analysis/compartment.csv",
                "/cytof_analysis/profiling.csv",
            ],
            "Keys for mapping from respective enumeration indices to the cell labels",
        ),
    },
    "WES": {
        "Source": FacetConfig(
            ["/wes/r1_.fastq.gz", "/wes/r2_.fastq.gz", "/wes/reads_.bam"]
        ),
        "Germline": FacetConfig(["/wes/analysis/vcfcompare.txt"]),
        "Purity": FacetConfig(["/wes/analysis/optimalpurityvalue.txt"]),
        "Clonality": FacetConfig(["/wes/analysis/clonality_pyclone.tsv"]),
        "Copy Number": FacetConfig(
            [
                "/wes/analysis/copynumber_cnvcalls.txt",
                "/wes/analysis/copynumber_cnvcalls.txt.tn.tsv",
            ]
        ),
        "Neoantigen": FacetConfig(
            [
                "/wes/analysis/MHC_Class_I_all_epitopes.tsv",
                "/wes/analysis/MHC_Class_I_filtered_condensed_ranked.tsv",
                "/wes/analysis/MHC_Class_II_all_epitopes.tsv",
                "/wes/analysis/MHC_Class_II_filtered_condensed_ranked.tsv",
            ]
        ),
        "Somatic": FacetConfig(
            [
                "/wes/analysis/vcf_tnscope_output.vcf",
                "/wes/analysis/maf_tnscope_output.maf",
                "/wes/analysis/vcf_tnscope_filter.vcf",
                "/wes/analysis/maf_tnscope_filter.maf",
                "/wes/analysis/tnscope_exons_broad.gz",
                "/wes/analysis/tnscope_exons_mda.gz",
                "/wes/analysis/tnscope_exons_mocha.gz",
            ]
        ),
        "Alignment": FacetConfig(
            [
                "/wes/analysis/tn_corealigned.bam",
                "/wes/analysis/tn_corealigned.bam.bai",
                "/wes/analysis/tumor/recalibrated.bam",
                "/wes/analysis/tumor/recalibrated.bam.bai",
                "/wes/analysis/tumor/sorted.dedup.bam",
                "/wes/analysis/tumor/sorted.dedup.bam.bai",
                "/wes/analysis/normal/recalibrated.bam",
                "/wes/analysis/normal/recalibrated.bam.bai",
                "/wes/analysis/normal/sorted.dedup.bam",
                "/wes/analysis/normal/sorted.dedup.bam.bai",
            ]
        ),
        "Metrics": FacetConfig(
            [
                "/wes/analysis/tumor/coverage_metrics.txt",
                "/wes/analysis/tumor/target_metrics.txt",
                "/wes/analysis/tumor/coverage_metrics_summary.txt",
                "/wes/analysis/tumor/target_metrics_summary.txt",
                "/wes/analysis/tumor/mosdepth_region_dist_broad.txt",
                "/wes/analysis/tumor/mosdepth_region_dist_mda.txt",
                "/wes/analysis/tumor/mosdepth_region_dist_mocha.txt",
                "/wes/analysis/normal/coverage_metrics.txt",
                "/wes/analysis/normal/target_metrics.txt",
                "/wes/analysis/normal/coverage_metrics_summary.txt",
                "/wes/analysis/normal/target_metrics_summary.txt",
                "/wes/analysis/normal/mosdepth_region_dist_broad.txt",
                "/wes/analysis/normal/mosdepth_region_dist_mda.txt",
                "/wes/analysis/normal/mosdepth_region_dist_mocha.txt",
            ]
        ),
        "HLA Type": FacetConfig(
            [
                "/wes/analysis/tumor/optitype_result.tsv",
                "/wes/analysis/normal/optitype_result.tsv",
            ]
        ),
        "Report": FacetConfig(["/wes/analysis/wes_version.txt"]),
    },
    "RNA": {
        "Source": FacetConfig(
            ["/rna/r1_.fastq.gz", "/rna/r2_.fastq.gz", "/rna/reads_.bam"]
        ),
        "Alignment": FacetConfig(
            [
                "/rna/analysis/star/sorted.bam",
                "/rna/analysis/star/sorted.bam.bai",
                "/rna/analysis/star/sorted.bam.stat.txt",
                "/rna/analysis/star/downsampling.bam",
                "/rna/analysis/star/downsampling.bam.bai",
            ]
        ),
        "Quality": FacetConfig(
            [
                "/rna/analysis/rseqc/downsampling_housekeeping.bam",
                "/rna/analysis/rseqc/downsampling_housekeeping.bam.bai",
                "/rna/analysis/rseqc/read_distrib.txt",
                "/rna/analysis/rseqc/tin_score.summary.txt",
                "/rna/analysis/rseqc/tin_score.txt",
            ]
        ),
        "Gene Quantification": FacetConfig(
            [
                "/rna/analysis/salmon/quant.sf",
                "/rna/analysis/salmon/transcriptome.bam.log",
                "/rna/analysis/salmon/aux_info_ambig_info.tsv",
                "/rna/analysis/salmon/aux_info_expected_bias.gz",
                "/rna/analysis/salmon/aux_info_meta_info.json",
                "/rna/analysis/salmon/aux_info_observed_bias.gz",
                "/rna/analysis/salmon/aux_info_observed_bias_3p.gz",
                "/rna/analysis/salmon/cmd_info.json",
                "/rna/analysis/salmon/salmon_quant.log",
            ]
        ),
    },
    "mIF": {
        "Source Images": FacetConfig(
            [
                "/mif/roi_/composite_image.tif",
                "/mif/roi_/component_data.tif",
                "/mif/roi_/multispectral.im3",
            ],
            "Image files containing the source multi-dimensional images for both ROIs and whole slide if appropriate.",
        ),
        "Analysis Images": FacetConfig(
            ["/mif/roi_/binary_seg_maps.tif", "/mif/roi_/phenotype_map.tif"],
            "Image-like files created or used in the analysis workflow. These include cell and region segmentation maps.",
        ),
        "Analysis Data": FacetConfig(
            [
                "/mif/roi_/score_data_.txt",
                "/mif/roi_/cell_seg_data.txt",
                "/mif/roi_/cell_seg_data_summary.txt",
            ],
            "Data files from image analysis software indicating the cell type assignments, phenotypes and other scoring metrics and thresholds.",
        ),
    },
    "Olink": {
        "All Olink Files": FacetConfig(
            [
                "Assay Type|Olink|All Olink Files|/olink",
                "/olink/study_npx.xlsx",
                "/olink/chip_/assay_npx.xlsx",
                "/olink/chip_/assay_raw_ct.csv",
            ],
            "Analysis files from the Olink platform.",
        )
    },
    "IHC": {
        "Images": FacetConfig(["/ihc/ihc_image."]),
        "Combined Markers": FacetConfig(["csv|ihc marker combined"]),
    },
}

clinical_facets: Facets = {
    "Participants Info": FacetConfig(
        ["Clinical Type|Participants Info|participants.csv", "csv|participants info"]
    ),
    "Samples Info": FacetConfig(
        ["Clinical Type|Samples Info|samples.csv", "csv|samples info"]
    ),
}

facets_dict: Dict[str, Facets] = {
    "Assay Type": assay_facets,
    "Clinical Type": clinical_facets,
}


FACET_NAME_DELIM = "|"


def _build_facet_groups_to_names(_facets=facets_dict):
    path_to_name = lambda path: FACET_NAME_DELIM.join(path)

    facet_names = {}

    for facet_name, subfacet in assay_facets.items():
        for subfacet_name, subsubfacet in subfacet.items():
            for facet_group in subsubfacet.match_clauses:
                facet_names[facet_group] = path_to_name([facet_name, subfacet_name])

    for facet_name, subfacet in clinical_facets.items():
        for facet_group in subfacet.match_clauses:
            facet_names[facet_group] = path_to_name([facet_name])

    return facet_names


facet_groups_to_names = _build_facet_groups_to_names()


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


def get_facet_groups_for_paths(paths: List[List[str]]) -> List[str]:
    facet_groups: List[str] = []
    for path in paths:
        try:
            assert len(path) in (2, 3)
            facet_config: Any = facets_dict
            for key in path:
                facet_config = facet_config[key]
            assert isinstance(facet_config, FacetConfig)
        except Exception as e:
            raise BadRequest(f"no facet for path {path}")
        facet_groups.extend(facet_config.match_clauses)

    return facet_groups
