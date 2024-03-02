from __future__ import annotations

import pytest

from utilities.text import EnsureStrError, ensure_str, strip_and_dedent


class TestEnsureStr:
    def test_str(self) -> None:
        assert isinstance(ensure_str(""), str)

    def test_not_str(self) -> None:
        with pytest.raises(
            EnsureStrError, match="Object .* must be a string; got .* instead"
        ):
            _ = ensure_str(None)


class TestStripAndDedent:
    def test_main(self) -> None:
        text = """
               This is line 1.
               This is line 2.
               """
        result = strip_and_dedent(text)
        expected = "This is line 1.\nThis is line 2."
        assert result == expected
