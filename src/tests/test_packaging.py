from __future__ import annotations

from packaging.requirements import InvalidRequirement
from pytest import raises

from utilities.packaging import SortedRequirement


class TestSortedRequirement:
    def test_error(self) -> None:
        with raises(InvalidRequirement, match="Expected end or semicolon"):
            _ = SortedRequirement("invalid >> 1.2.3")
