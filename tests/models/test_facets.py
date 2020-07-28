from unittest.mock import MagicMock

import pytest
from werkzeug.exceptions import BadRequest

from cidc_api.models.facets import facets, get_facet_labels, get_facets_for_paths


def test_get_facet_labels():
    """Ensure get_facet_labels returns an object with the expected structure."""
    labels = get_facet_labels()
    for value in labels.values():
        if isinstance(value, dict):
            for subvalue in value.values():
                assert isinstance(subvalue, list)
        else:
            assert isinstance(value, list)


def test_get_facets_for_paths():
    """Test that get_facets_for_paths works as expected."""
    mock_like = lambda v: v

    assert get_facets_for_paths(mock_like, []) == []

    # Existing paths
    good_paths = [
        ["Assay Type", "WES", "Somatic"],
        ["Assay Type", "RNA", "Quality"],
        ["Clinical Type", "Participants Info"],
    ]
    facets_for_paths = get_facets_for_paths(mock_like, good_paths)
    assert facets_for_paths == [
        *facets["Assay Type"]["WES"]["Somatic"],
        *facets["Assay Type"]["RNA"]["Quality"],
        *facets["Clinical Type"]["Participants Info"],
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
            get_facets_for_paths(mock_like, [path])
