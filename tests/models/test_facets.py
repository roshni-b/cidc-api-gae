from unittest.mock import MagicMock

import pytest
from werkzeug.exceptions import BadRequest

from cidc_api.models.facets import (
    facets,
    get_facet_info,
    get_facet_groups_for_paths,
    FacetConfig,
)


def test_get_facet_info():
    """Ensure get_facet_labels returns an object with the expected structure."""

    def test_info_structure(config: dict):
        assert "label" in config
        assert "description" in config

    labels = get_facet_info()
    for value in labels.values():
        if isinstance(value, dict):
            for subvalue in value.values():
                assert isinstance(subvalue, list)
                for config in subvalue:
                    test_info_structure(config)
        else:
            assert isinstance(value, list)
            for config in value:
                test_info_structure(config)


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
        *facets["Assay Type"]["WES"]["Somatic"].match_clauses,
        *facets["Assay Type"]["RNA"]["Quality"].match_clauses,
        *facets["Clinical Type"]["Participants Info"].match_clauses,
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
