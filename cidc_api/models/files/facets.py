from typing import Dict, List, Optional, Set, Union, NamedTuple, Any

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
    "Nanostring": {
        "Source": FacetConfig(
            ["/nanostring/.rcc", "/nanostring/control.rcc"],
            "direct output from a single NanoString run",
        ),
        "Data": FacetConfig(
            ["/nanostring/raw_data.csv", "/nanostring/normalized_data.csv"],
            "tabulated data across all samples in a batch",
        ),
    },
    "CyTOF": {
        "Source": FacetConfig(
            [
                "/cytof_10021/spike_in.fcs",
                "/cytof_10021/source_.fcs",
                "/cytof_10021/normalized_and_debarcoded.fcs",
                "/cytof_10021/processed.fcs",
                "/cytof_e4412/spike_in.fcs",
                "/cytof_e4412/source_.fcs",
                "/cytof_e4412/normalized_and_debarcoded.fcs",
                "/cytof_e4412/processed.fcs",
            ],
            "De-barcoded, concatenated and de-multipled fcs files",
        ),
        "Cell Counts": FacetConfig(
            [
                "/cytof_10021_analysis/cell_counts_assignment.csv",
                "/cytof_10021_analysis/cell_counts_compartment.csv",
                "/cytof_10021_analysis/cell_counts_profiling.csv",
                "/cytof_e4412_analysis/cell_counts_assignment.csv",
                "/cytof_e4412_analysis/cell_counts_compartment.csv",
                "/cytof_e4412_analysis/cell_counts_profiling.csv",
            ],
            "Summary cell count expression of individual cell types in each sample",
        ),
        "Combined Cell Counts": FacetConfig(
            [
                "csv|cell counts compartment",
                "csv|cell counts assignment",
                "csv|cell counts profiling",
            ],
            "Summary cell counts, combined across all samples in the trial",
        ),
        "Labeled Source": FacetConfig(
            ["/cytof_10021_analysis/source.fcs", "/cytof_e4412_analysis/source.fcs"],
            "FCS file with enumerations for compartment, assignment and profiling cell labels",
        ),
        "Analysis Results": FacetConfig(
            [
                "/cytof_10021_analysis/reports.zip",
                "/cytof_10021_analysis/analysis.zip",
                "/cytof_e4412_analysis/reports.zip",
                "/cytof_e4412_analysis/analysis.zip",
            ],
            "Results package from Astrolabe analysis",
        ),
        "Key": FacetConfig(
            [
                "/cytof_10021_analysis/assignment.csv",
                "/cytof_10021_analysis/compartment.csv",
                "/cytof_10021_analysis/profiling.csv",
                "/cytof_e4412_analysis/assignment.csv",
                "/cytof_e4412_analysis/compartment.csv",
                "/cytof_e4412_analysis/profiling.csv",
            ],
            "Keys for mapping from respective enumeration indices to the cell labels",
        ),
    },
    "WES": {
        "Source": FacetConfig(
            ["/wes/r1_.fastq.gz", "/wes/r2_.fastq.gz", "/wes/reads_.bam"]
        ),
        "Germline": FacetConfig(
            [
                "/wes/analysis/vcfcompare.txt",
                "/wes/analysis/tumor/haplotyper_targets.vcf.gz",
                "/wes/analysis/normal/haplotyper_targets.vcf.gz",
            ]
        ),
        "Purity": FacetConfig(["/wes/analysis/optimal_purity_value.txt"]),
        "Clonality": FacetConfig(
            ["/wes/analysis/clonality_pyclone.tsv", "/wes/analysis/clonality_table.tsv"]
        ),
        "Copy Number": FacetConfig(
            [
                "/wes/analysis/copynumber_cnvcalls.txt",
                "/wes/analysis/copynumber_cnvcalls.txt.tn.tsv",
            ]
        ),
        "Neoantigen": FacetConfig(
            ["/wes/analysis/HLA_results.tsv", "/wes/analysis/combined_filtered.tsv"]
        ),
        "Somatic": FacetConfig(
            [
                "/wes/analysis/vcf_gz_tnscope_output.vcf.gz",
                "/wes/analysis/maf_tnscope_output.maf",
                "/wes/analysis/vcf_gz_tnscope_filter.vcf.gz",
                "/wes/analysis/maf_tnscope_filter.maf",
                "/wes/analysis/tnscope_exons_broad.vcf.gz",
                "/wes/analysis/tnscope_exons_mda.vcf.gz",
                "/wes/analysis/tnscope_exons_mocha.vcf.gz",
            ]
        ),
        "Alignment": FacetConfig(
            [
                "/wes/analysis/tumor/sorted.dedup.bam",
                "/wes/analysis/tumor/sorted.dedup.bam.bai",
                "/wes/analysis/normal/sorted.dedup.bam",
                "/wes/analysis/normal/sorted.dedup.bam.bai",
            ]
        ),
        "Metrics": FacetConfig(
            [
                "/wes/analysis/tumor/coverage_metrics.txt",
                "/wes/analysis/tumor/target_metrics.txt",
                "/wes/analysis/tumor/coverage_metrics_summary.txt",
                "/wes/analysis/normal/coverage_metrics.txt",
                "/wes/analysis/normal/target_metrics.txt",
                "/wes/analysis/normal/coverage_metrics_summary.txt",
            ]
        ),
        "HLA Type": FacetConfig(
            [
                "/wes/analysis/tumor/optitype_result.tsv",
                "/wes/analysis/normal/optitype_result.tsv",
            ]
        ),
        "Report": FacetConfig(
            [
                "/wes/analysis/wes_version.txt",
                "/wes/analysis/tumor_mutational_burden.tsv",
                "/wes/analysis/report.tar.gz",
                "/wes/analysis/wes_run_version.tsv",
                "/wes/analysis/config.yaml",
                "/wes/analysis/metasheet.csv",
                "/wes/analysis/wes_sample.json",
                "/wes/analysis/xhla_report_hla.json",
            ]
        ),
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
        "Run-Level": FacetConfig(
            [
                "/olink/batch_/chip_/assay_npx.xlsx",
                "/olink/batch_/chip_/assay_raw_ct.csv",
                "/olink/batch_/combined_npx.xlsx",
            ],
            "Analysis files for a single run on the Olink platform.",
        ),
        "Study-Level": FacetConfig(
            ["/olink/study_npx.xlsx", "npx|analysis_ready|csv"],
            "Analysis files for all samples run on the Olink platform in the trial.",
        ),
    },
    "IHC": {
        "Images": FacetConfig(["/ihc/ihc_image."]),
        "Combined Markers": FacetConfig(["csv|ihc marker combined"]),
    },
    "H&E": {"Images": FacetConfig(["/hande/image_file.svs"], "Stained image file.")},
    "TCR": {
        "Source": FacetConfig(
            [
                "/tcr/replicate_/r1.fastq.gz",
                "/tcr/replicate_/r2.fastq.gz",
                "/tcr/replicate_/i1.fastq.gz",
                "/tcr/replicate_/i2.fastq.gz",
            ]
        ),
        "Misc.": FacetConfig(["/tcr/SampleSheet.csv" "/tcr/summary_info.csv"]),
        "Analysis Data": FacetConfig(
            ["/tcr_analysis/tra_clone.csv", "/tcr_analysis/trb_clone.csv"],
            "Data files from TCRseq analysis indicating TRA & TRB clones UMI counts",
        ),
    },
    "ELISA": {"Data": FacetConfig(["/elisa/assay.xlsx"])},
}

clinical_facets: Facets = {
    "Participants Info": FacetConfig(
        ["Clinical Type|Participants Info|participants.csv", "csv|participants info"]
    ),
    "Samples Info": FacetConfig(
        ["Clinical Type|Samples Info|samples.csv", "csv|samples info"]
    ),
}

analysis_ready_facets = {"Olink": FacetConfig(["npx|analysis_ready|csv"])}

facets_dict: Dict[str, Facets] = {
    "Assay Type": assay_facets,
    "Clinical Type": clinical_facets,
    "Analysis Ready": analysis_ready_facets,
}


FACET_NAME_DELIM = "|"


def _build_facet_groups_to_names():
    path_to_name = lambda path: FACET_NAME_DELIM.join(path)

    facet_names = {}

    for facet_type, facet_dict in facets_dict.items():
        for facet_name, subfacet in facet_dict.items():
            if isinstance(subfacet, dict):
                for subfacet_name, subsubfacet in subfacet.items():
                    for facet_group in subsubfacet.match_clauses:
                        facet_names[facet_group] = path_to_name(
                            [facet_name, subfacet_name]
                        )

            elif isinstance(subfacet, FacetConfig):
                for facet_group in subfacet.match_clauses:
                    facet_names[facet_group] = path_to_name([facet_name])

    return facet_names


facet_groups_to_names = _build_facet_groups_to_names()


def build_data_category_facets(data_category_file_counts: Dict[str, int]):
    """
    Add file counts by data category into the facets defined in the `facets_dict`,
    and reformat `FacetConfig`s as facet specification dictionaries with the following structure:
    ```python
    {
        "label": <the display name for this facet>,
        "description": <background info for this facet>,
        "count": <number of files related to this facet>
    }
    ```
    """
    extract_facet_info = lambda facet_config_entries, prefix: [
        {
            "label": label,
            "description": config.description,
            "count": data_category_file_counts.get(
                FACET_NAME_DELIM.join([prefix, label]) if prefix else label, 0
            ),
        }
        for label, config in facet_config_entries.items()
    ]

    return {
        "Assay Type": {
            assay_name: extract_facet_info(subfacets, assay_name)
            for assay_name, subfacets in assay_facets.items()
        },
        "Clinical Type": extract_facet_info(clinical_facets, None),
        "Analysis Ready": extract_facet_info(analysis_ready_facets, None),
    }


def build_trial_facets(trial_file_counts: Dict[str, int]):
    """
    Convert a mapping from trial ids to file counts into a list of facet specifications.
    """
    return [
        {"label": trial_id, "count": count}
        for trial_id, count in trial_file_counts.items()
    ]


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
