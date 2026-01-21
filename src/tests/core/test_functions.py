from __future__ import annotations

from hypothesis import given
from hypothesis.strategies import integers

from utilities.core import first, identity, last, second


class TestFirst:
    @given(x=integers(), y=integers())
    def test_main(self, *, x: int, y: int) -> None:
        pair = x, y
        result = first(pair)
        assert result == x


class TestIdentity:
    @given(x=integers())
    def test_main(self, *, x: int) -> None:
        assert identity(x) == x


class TestLast:
    @given(x=integers(), y=integers())
    def test_main(self, *, x: int, y: int) -> None:
        pair = x, y
        assert last(pair) == y


class TestSecond:
    @given(x=integers(), y=integers())
    def test_main(self, *, x: int, y: int) -> None:
        pair = x, y
        assert second(pair) == y
