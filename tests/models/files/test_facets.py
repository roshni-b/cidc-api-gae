import pytest
from werkzeug.exceptions import BadRequest

from cidc_api.models.files.facets import (
    facets_dict,
    build_trial_facets,
    build_data_category_facets,
    get_facet_groups_for_paths,
    FacetConfig,
)


def test_build_data_category_facets():
    """Ensure build_data_category_facets works as expected."""

    wes_count = 5
    sample_count = 12
    data_category_file_counts = {"WES|Source": wes_count, "Samples Info": sample_count}

    def assert_expected_facet_structure(config: dict, count: int = 0):
        assert "label" in config
        assert "description" in config
        assert config["count"] == count

    facet_specs = build_data_category_facets(data_category_file_counts)
    for value in facet_specs.values():
        if isinstance(value, dict):
            for value_key, subvalue in value.items():
                assert isinstance(subvalue, (list, FacetConfig))
                if isinstance(subvalue, dict):
                    for config in subvalue:
                        if value_key == "WES" and config["label"] == "Source":
                            assert_expected_facet_structure(config, wes_count)
                        else:
                            assert_expected_facet_structure(config)
                elif isinstance(subvalue, FacetConfig):
                    assert_expected_facet_structure(subvalue)

        else:
            assert isinstance(value, list)
            for config in value:
                if config["label"] == "Samples Info":
                    assert_expected_facet_structure(config, sample_count)
                else:
                    assert_expected_facet_structure(config)


def test_build_trial_facets():
    """Ensure build_trial_facets works as expected."""
    trial_file_counts = {"t1": 1, "t2": 2, "t3": 3}
    trial_facets = build_trial_facets(trial_file_counts)
    assert trial_facets == [
        {"label": "t1", "count": trial_file_counts["t1"]},
        {"label": "t2", "count": trial_file_counts["t2"]},
        {"label": "t3", "count": trial_file_counts["t3"]},
    ]


def test_get_facet_groups_for_paths():
    """Test that get_facet_groups_for_paths works as expected."""

    assert get_facet_groups_for_paths([]) == []

    # Existing paths
    good_paths = [
        ["Assay Type", "WES", "Somatic"],
        ["Assay Type", "RNA", "Quality"],
        ["Clinical Type", "Participants Info"],
    ]
    facets_for_paths = get_facet_groups_for_paths(good_paths)
    assert facets_for_paths == [
        *facets_dict["Assay Type"]["WES"]["Somatic"].match_clauses,
        *facets_dict["Assay Type"]["RNA"]["Quality"].match_clauses,
        *facets_dict["Clinical Type"]["Participants Info"].match_clauses,
    ]

    # Non-existent paths
    bad_paths = [
        ["foo"],
        ["Assay Type"],
        ["Assay Type", "WES"],
        ["Clinical Type", "Participants Info", "Foo"],
    ]
    for path in bad_paths:
        with pytest.raises(BadRequest, match=f"no facet for path"):
            get_facet_groups_for_paths([path])
