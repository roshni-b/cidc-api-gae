from typing import List


def assert_same_elements(a: List, b: List):
    """Check if `a` and `b` contain the same elements, raising an assertion error if not."""
    assert len(a) == len(b)
    assert set(a) == set(b)
