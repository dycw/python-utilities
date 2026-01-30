from __future__ import annotations

from math import exp

from utilities.core import indent_non_head


class TestIndentNonHead:
    @mark.parametrize("argname", [param(argvalue)])
    def test_main(self, *, text: str, expected: str) -> None:
        assert indent_non_head(text) == expected
