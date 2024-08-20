from __future__ import annotations

from operator import sub

from hypothesis import given
from hypothesis.strategies import integers

from utilities.functools import partial


class TestPartial:
    @given(x=integers(), y=integers())
    def test_main(self, *, x: int, y: int) -> None:
        func = partial(sub, ..., y)
        assert func(x) == x - y
