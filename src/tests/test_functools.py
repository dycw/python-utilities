from __future__ import annotations

from functools import reduce
from operator import add, sub

from hypothesis import given
from hypothesis.strategies import integers
from pytest import raises

from utilities.functools import EmptyReduceError, partial, redirect_empty_reduce


class TestPartial:
    @given(x=integers(), y=integers())
    def test_main(self, *, x: int, y: int) -> None:
        func = partial(sub, ..., y)
        assert func(x) == x - y


class TestReduce:
    def test_main(self) -> None:
        with raises(EmptyReduceError), redirect_empty_reduce():
            _ = reduce(add, [])
