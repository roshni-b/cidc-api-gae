from collections import OrderedDict
from functools import wraps
from unittest.mock import MagicMock

from flask.globals import session

from cidc_api.models.templates import in_single_transaction, TEMPLATE_MAP


def test_in_single_transaction_smoketest(cidc_api):
    session = MagicMock()
    func1, func2 = MagicMock(), MagicMock()
    func1.return_value, func2.return_value = [], []

    def func1_assert(**kwargs):
        func1.assert_called_once_with(**kwargs)
        return []

    def func2_assert(**kwargs):
        func2.assert_not_called()
        return []

    calls = OrderedDict()
    calls[func1] = {"foo": "bar"}
    calls[func1_assert] = {"foo": "bar"}
    calls[func2_assert] = {}
    calls[func2] = {}

    errs = in_single_transaction(calls, session=session)
    assert len(errs) == 0, "\n".join(str(e) for e in errs)

    func1.assert_called_once_with(foo="bar", session=session, hold_commit=True)
    func2.assert_called_once_with(session=session, hold_commit=True)
    session.commit.assert_called_once()
    session.flush.assert_called()


def test_get_full_template_name():
    assert list(TEMPLATE_MAP.keys()) == [
        "hande",
        "wes_fastq",
        "wes_bam",
        "pbmc",
        "tissue_slide",
    ]
