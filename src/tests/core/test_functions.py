from __future__ import annotations

from operator import add

from hypothesis import given
from hypothesis.strategies import booleans, integers

from utilities.core import apply, first, identity, last, second, to_bool
from utilities.hypothesis import pairs, quadruples, triples


class TestApply:
    def test_main(self) -> None:
        assert apply(add, 1, 2) == 3


class TestFirst:
    @given(x=integers())
    def test_single(self, *, x: int) -> None:
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
    @given(x=integers())
    def test_single(self, *, x: int) -> None:
        assert last((x,)) == x

    @given(x=pairs(integers()))
    def test_pair(self, *, x: tuple[int, int]) -> None:
        assert last(x) == x[-1]

    @given(x=triples(integers()))
    def test_triple(self, *, x: tuple[int, int, int]) -> None:
        assert last(x) == x[-1]

    @given(x=quadruples(integers()))
    def test_quadruple(self, *, x: tuple[int, int, int, int]) -> None:
        assert last(x) == x[-1]


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


class TestToBool:
    @given(bool_=booleans())
    def test_bool(self, *, bool_: bool) -> None:
        assert to_bool(bool_) is bool_

    @given(bool_=booleans())
    def test_str(self, *, bool_: bool) -> None:
        assert to_bool(str(bool_)) is bool_

    @given(bool_=booleans())
    def test_callable(self, *, bool_: bool) -> None:
        assert to_bool(lambda: bool_) is bool_
