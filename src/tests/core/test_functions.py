from __future__ import annotations

from hypothesis import given
from hypothesis.strategies import integers

from utilities.core import first, identity, last, second
from utilities.hypothesis import pairs, quadruples, triples


class TestFirst:
    @given(x=integers())
    def test_main(self, *, x: int) -> None:
        assert first((x,)) == x

    @given(x=pairs(integers()))
    def test_pair(self, *, x: tuple[int, int]) -> None:
        assert first(x) == x[0]

    @given(x=triples(integers()))
    def test_triple(self, *, x: tuple[int, int, int]) -> None:
        assert first(x) == x[0]

    @given(x=quadruples(integers()))
    def test_quadruple(self, *, x: tuple[int, int, int, int]) -> None:
        assert first(x) == x[0]


class TestIdentity:
    @given(x=integers())
    def test_main(self, *, x: int) -> None:
        assert identity(x) == x


class TestLast:
    @given(x=integers(), y=integers())
    def test_main(self, *, x: int, y: int) -> None:
        assert last((x, y)) == y


class TestSecond:
    @given(x=pairs(integers()))
    def test_pair(self, *, x: tuple[int, int]) -> None:
        assert second(x) == x[1]

    @given(x=triples(integers()))
    def test_triple(self, *, x: tuple[int, int, int]) -> None:
        assert second(x) == x[1]

    @given(x=quadruples(integers()))
    def test_quadruple(self, *, x: tuple[int, int, int, int]) -> None:
        assert second(x) == x[1]
