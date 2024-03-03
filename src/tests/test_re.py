from __future__ import annotations

from re import DOTALL
from typing import TYPE_CHECKING

import pytest

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
    from utilities.types import IterableStrs


class TestExtractGroup:
    def test_main(self) -> None:
        assert extract_group(r"(\d)", "A0A") == "0"

    def test_with_flags(self) -> None:
        assert extract_group(r"(.\d)", "A\n0\nA", flags=DOTALL) == "\n0"

    @pytest.mark.parametrize(
        ("pattern", "text", "error", "match"),
        [
            pytest.param(
                r"\d",
                "0",
                _ExtractGroupNoCaptureGroupsError,
                "Pattern .* must contain exactly one capture group; it had none",
            ),
            pytest.param(
                r"(\d)(\w)",
                "0A",
                _ExtractGroupMultipleCaptureGroupsError,
                "Pattern .* must contain exactly one capture group; it had multiple",
            ),
            pytest.param(
                r"(\d)",
                "A",
                _ExtractGroupNoMatchesError,
                "Pattern .* must match against .*",
            ),
            pytest.param(
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
        with pytest.raises(error, match=match):
            _ = extract_group(pattern, text)


class TestExtractGroups:
    @pytest.mark.parametrize(
        ("pattern", "text", "expected"),
        [
            pytest.param(r"(\d)", "A0A", ["0"]),
            pytest.param(r"(\d)(\w)", "A0A0", ["0", "A"]),
        ],
    )
    def test_main(self, *, pattern: str, text: str, expected: IterableStrs) -> None:
        assert extract_groups(pattern, text) == expected

    def test_with_flags(self) -> None:
        assert extract_groups(r"(.)(\d)(\w)", "\n0A\n", flags=DOTALL) == [
            "\n",
            "0",
            "A",
        ]

    @pytest.mark.parametrize(
        ("pattern", "text", "error", "match"),
        [
            pytest.param(
                r"\d",
                "0",
                _ExtractGroupsNoCaptureGroupsError,
                "Pattern .* must contain at least one capture group",
            ),
            pytest.param(
                r"(\d)",
                "A",
                _ExtractGroupsNoMatchesError,
                "Pattern .* must match against .*",
            ),
            pytest.param(
                r"(\d)",
                "0A0",
                _ExtractGroupsMultipleMatchesError,
                r"Pattern .* must match against .* exactly once; matches were \[.*, .*\]",
            ),
            pytest.param(
                r"(\d)(\w)",
                "A0",
                _ExtractGroupsNoMatchesError,
                "Pattern .* must match against .*",
            ),
            pytest.param(
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
        with pytest.raises(error, match=match):
            _ = extract_groups(pattern, text)
