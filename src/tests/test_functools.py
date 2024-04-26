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


class TestRedirectEmptyReduce:
    def test_main(self) -> None:
        with (
            raises(
                EmptyReduceError,
                match=r"reduce\(\) must not be called over an empty iterable, or must have an initial value\.",
            ),
            redirect_empty_reduce(),
        ):
            _ = reduce(add, [])

    def test_other_error(self) -> None:
        def run_test() -> None:
            with redirect_empty_reduce():
                msg = "other"
                raise TypeError(msg)

        with raises(TypeError, match="other"):
            run_test()
