from unittest.mock import MagicMock
import pytest

from cidc_api.models import MetadataModel


def test_metadata_model():
    md = MetadataModel()
    tbl = MagicMock()
    tbl.columns = [
        MagicMock(primary_key=True, unique=True, name="foo"),
        MagicMock(primary_key=False, unique=True, name="bar"),
        MagicMock(primary_key=False, unique=False, name="baz"),
    ]
    tbl.columns[0].name = "foo"
    tbl.columns[1].name = "bar"
    tbl.columns[1].primary_key = False
    tbl.columns[2].name = "baz"
    tbl.columns[2].primary_key = False
    tbl.columns[2].unique = False

    setattr(md, "__table__", tbl)
    setattr(md, "__tablename__", "table")
    setattr(md, "foo", "foo")
    setattr(md, "bar", "bar")
    setattr(md, "baz", "baz")

    assert md.unique_field_values() == ("foo", "bar")

    with pytest.raises(Exception, match="cannot merge"):
        md.merge(MagicMock())

    other = MetadataModel()
    setattr(other, "__table__", tbl)
    setattr(other, "foo", None)
    setattr(other, "bar", "bar")
    setattr(other, "baz", None)
    other.merge(md)
    assert other.foo == "foo"
    assert other.bar == "bar"
    assert other.baz == "baz"

    setattr(other, "bar", "bag")
    with pytest.raises(Exception, match="found conflicting values for"):
        md.merge(other)
