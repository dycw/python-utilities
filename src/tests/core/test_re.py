from __future__ import annotations

from re import DOTALL, escape

from pytest import mark, param, raises

from utilities._core_errors import (
    CheckMultiLineRegexNoMatchError,
    CheckMultiLineRegexNumberOfLinesError,
)
from utilities.core import (
    ExtractGroupMultipleCaptureGroupsError,
    ExtractGroupMultipleMatchesError,
    ExtractGroupNoCaptureGroupsError,
    ExtractGroupNoMatchesError,
    ExtractGroupsMultipleMatchesError,
    ExtractGroupsNoCaptureGroupsError,
    ExtractGroupsNoMatchesError,
    check_multi_line_regex,
    extract_group,
    extract_groups,
    normalize_multi_line_str,
)


class TestCheckMultiLineRegex:
    def test_main(self) -> None:
        pattern = normalize_multi_line_str("""
            [A-Z]+
            [a-z]+
            [0-9]+
        """)
        text = normalize_multi_line_str("""
            ABC
            def
            123
        """)
        check_multi_line_regex(pattern, text)

    def test_error_no_match(self) -> None:
        pattern = normalize_multi_line_str("""
            [A-Z]+
            [a-z]+
            [0-9]+
        """)
        text = normalize_multi_line_str("""
            ABC
            123
            def
        """)
        with raises(
            CheckMultiLineRegexNoMatchError,
            match=escape(r"Line 2: pattern '[a-z]+' must match against '123'"),
        ):
            check_multi_line_regex(pattern, text)

    @mark.parametrize(
        ("pattern", "text"),
        [
            param(
                """
                    [A-Z]+
                    [a-z]+
                    [0-9]+
                """,
                """
                    ABC
                """,
            ),
            param(
                """
                    [A-Z]+
                """,
                """
                    ABC
                    def
                    123
                """,
            ),
        ],
    )
    def test_error_number_of_lines(self, *, pattern: str, text: str) -> None:
        pattern = normalize_multi_line_str(pattern)
        text = normalize_multi_line_str(text)
        with raises(
            CheckMultiLineRegexNumberOfLinesError,
            match=r"Pattern '.*' and text '.*' must contain the same number of lines; got \d+ and \d+",
        ):
            check_multi_line_regex(pattern, text)


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
                ExtractGroupNoCaptureGroupsError,
                'Pattern ".*" must contain exactly one capture group; it had none',
            ),
            param(
                r"(\d)(\w)",
                "0A",
                ExtractGroupMultipleCaptureGroupsError,
                'Pattern ".*" must contain exactly one capture group; it had multiple',
            ),
            param(
                r"(\d)",
                "A",
                ExtractGroupNoMatchesError,
                """Pattern ".*" must match against '.*'""",
            ),
            param(
                r"(\d)",
                "0A0",
                ExtractGroupMultipleMatchesError,
                r"""Pattern ".*" must match against '.*' exactly once; matches were \[.*, .*\]""",
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
    def test_main(self, *, pattern: str, text: str, expected: list[str]) -> None:
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
                ExtractGroupsNoCaptureGroupsError,
                "Pattern .* must contain at least one capture group",
            ),
            param(
                r"(\d)",
                "A",
                ExtractGroupsNoMatchesError,
                "Pattern .* must match against '.*'",
            ),
            param(
                r"(\d)",
                "0A0",
                ExtractGroupsMultipleMatchesError,
                r"""Pattern ".*" must match against '.*' exactly once; matches were \[.*, .*\]""",
            ),
            param(
                r"(\d)(\w)",
                "A0",
                ExtractGroupsNoMatchesError,
                "Pattern .* must match against '.*'",
            ),
            param(
                r"(\d)(\w)",
                "0A0A",
                ExtractGroupsMultipleMatchesError,
                r"""Pattern ".*" must match against '.*' exactly once; matches were \[.*, .*\]""",
            ),
        ],
    )
    def test_errors(
        self, *, pattern: str, text: str, error: type[Exception], match: str
    ) -> None:
        with raises(error, match=match):
            _ = extract_groups(pattern, text)
