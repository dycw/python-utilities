from __future__ import annotations

from pytest import raises
from typing_extensions import Self

from utilities.text import EnsureStrError, ensure_str, strip_and_dedent


class TestEnsureStr:
    def test_str(self: Self) -> None:
        assert isinstance(ensure_str(""), str)

    def test_not_str(self: Self) -> None:
        with raises(EnsureStrError, match="Object .* must be a string; got .* instead"):
            _ = ensure_str(None)


class TestStripAndDedent:
    def test_main(self: Self) -> None:
        text = """
               This is line 1.
               This is line 2.
               """
        result = strip_and_dedent(text)
        expected = "This is line 1.\nThis is line 2."
        assert result == expected
