from cidc_api.models.templates import TEMPLATE_MAP


def test_get_full_template_name():
    assert list(TEMPLATE_MAP.keys()) == [
        "hande",
        "wes_fastq",
        "wes_bam",
        "pbmc",
        "tissue_slide",
    ]
