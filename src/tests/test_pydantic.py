from __future__ import annotations

from hypothesis import given
from hypothesis.strategies import integers

from utilities.pydantic import HashableBaseModel


class TestHashableBaseModel:
    @given(x=integers())
    def test_main(self, *, x: int) -> None:
        class Example(HashableBaseModel):
            x: int

        example = Example(x=x)
        assert isinstance(hash(example), int)
