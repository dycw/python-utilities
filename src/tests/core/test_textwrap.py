from __future__ import annotations

from pytest import mark, param

from utilities.core import indent_non_head


class TestIndentNonHead:
    @mark.parametrize(
        ("text", "expected"),
        [
            param("", ""),
            param("line 1", "line 1"),
            param("line 1\n", "line 1\n"),
            param("line 1\n\n", "line 1\n\n"),
            param("line 1\nline 2", "line 1\n  line 2"),
            param("line 1\nline 2\nline 3", "line 1\n  line 2\n  line 3"),
        ],
    )
    def test_main(self, *, text: str, expected: str) -> None:
        assert indent_non_head(text, "  ") == expected
