from __future__ import annotations

from hypothesis import given
from hypothesis.strategies import integers

from utilities.functions import identity


class TestIdentity:
    @given(x=integers())
    def test_main(self, *, x: int) -> None:
        assert identity(x) == x
