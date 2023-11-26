from __future__ import annotations

from pytest import mark, param, raises

from utilities.re import (
    ExtractGroupError,
    ExtractGroupsError,
    extract_group,
    extract_groups,
)
from utilities.types import IterableStrs


class TestExtractGroup:
    def test_main(self) -> None:
        assert extract_group(r"(\d)", "A0A") == "0"

    @mark.parametrize(
        ("pattern", "text"),
        [
            param(r"\d", "0", id="no groups"),
            param(r"(\d)(\w)", "0A", id="multiple groups"),
            param(r"(\d)", "A", id="no matches"),
            param(r"(\d)", "0A0", id="multiple matches"),
        ],
    )
    def test_errors(self, *, pattern: str, text: str) -> None:
        with raises(ExtractGroupError):
            _ = extract_group(pattern, text)


class TestExtractGroups:
    @mark.parametrize(
        ("pattern", "text", "expected"),
        [param(r"(\d)", "A0A", ["0"]), param(r"(\d)(\w)", "A0A0", ["0", "A"])],
    )
    def test_main(self, *, pattern: str, text: str, expected: IterableStrs) -> None:
        assert extract_groups(pattern, text) == expected

    @mark.parametrize(
        ("pattern", "text"),
        [
            param(r"\d", "0", id="no groups"),
            param(r"(\d)", "A", id="one group, no matches"),
            param(r"(\d)", "0A0", id="one group, multiple matches"),
            param(r"(\d)(\w)", "A0", id="multiple groups, no matches"),
            param(r"(\d)(\w)", "0A0A", id="multiple groups, multiple matches"),
        ],
    )
    def test_errors(self, *, pattern: str, text: str) -> None:
        with raises(ExtractGroupsError):
            _ = extract_groups(pattern, text)
