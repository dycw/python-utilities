from __future__ import annotations

from re import DOTALL
from typing import TYPE_CHECKING

from pytest import mark, param, raises

from utilities.re import (
    _ExtractGroupMultipleCaptureGroupsError,
    _ExtractGroupMultipleMatchesError,
    _ExtractGroupNoCaptureGroupsError,
    _ExtractGroupNoMatchesError,
    _ExtractGroupsMultipleMatchesError,
    _ExtractGroupsNoCaptureGroupsError,
    _ExtractGroupsNoMatchesError,
    extract_group,
    extract_groups,
)

if TYPE_CHECKING:
    from collections.abc import Sequence


class TestExtractGroup:
    def test_main(self) -> None:
        assert extract_group(r"(\d)", "A0A") == "0"

    def test_with_flags(self) -> None:
        assert extract_group(r"(.\d)", "A\n0\nA", flags=DOTALL) == "\n0"

    @mark.parametrize(
        ("pattern", "text", "error", "match"),
        [
            param(
                r"\d",
                "0",
                _ExtractGroupNoCaptureGroupsError,
                "Pattern .* must contain exactly one capture group; it had none",
            ),
            param(
                r"(\d)(\w)",
                "0A",
                _ExtractGroupMultipleCaptureGroupsError,
                "Pattern .* must contain exactly one capture group; it had multiple",
            ),
            param(
                r"(\d)",
                "A",
                _ExtractGroupNoMatchesError,
                "Pattern .* must match against .*",
            ),
            param(
                r"(\d)",
                "0A0",
                _ExtractGroupMultipleMatchesError,
                "Pattern .* must match against .* exactly once; matches were .*",
            ),
        ],
    )
    def test_errors(
        self, *, pattern: str, text: str, error: type[Exception], match: str
    ) -> None:
        with raises(error, match=match):
            _ = extract_group(pattern, text)


class TestExtractGroups:
    @mark.parametrize(
        ("pattern", "text", "expected"),
        [param(r"(\d)", "A0A", ["0"]), param(r"(\d)(\w)", "A0A0", ["0", "A"])],
    )
    def test_main(self, *, pattern: str, text: str, expected: Sequence[str]) -> None:
        assert extract_groups(pattern, text) == expected

    def test_with_flags(self) -> None:
        assert extract_groups(r"(.)(\d)(\w)", "\n0A\n", flags=DOTALL) == [
            "\n",
            "0",
            "A",
        ]

    @mark.parametrize(
        ("pattern", "text", "error", "match"),
        [
            param(
                r"\d",
                "0",
                _ExtractGroupsNoCaptureGroupsError,
                "Pattern .* must contain at least one capture group",
            ),
            param(
                r"(\d)",
                "A",
                _ExtractGroupsNoMatchesError,
                "Pattern .* must match against .*",
            ),
            param(
                r"(\d)",
                "0A0",
                _ExtractGroupsMultipleMatchesError,
                r"Pattern .* must match against .* exactly once; matches were \[.*, .*\]",
            ),
            param(
                r"(\d)(\w)",
                "A0",
                _ExtractGroupsNoMatchesError,
                "Pattern .* must match against .*",
            ),
            param(
                r"(\d)(\w)",
                "0A0A",
                _ExtractGroupsMultipleMatchesError,
                r"Pattern .* must match against .* exactly once; matches were \[.*, .*\]",
            ),
        ],
    )
    def test_errors(
        self, *, pattern: str, text: str, error: type[Exception], match: str
    ) -> None:
        with raises(error, match=match):
            _ = extract_groups(pattern, text)
