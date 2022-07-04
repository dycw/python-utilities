from enum import auto

from hypothesis import given
from hypothesis.strategies import sampled_from

from utilities.enum import StrEnum


class Truth(StrEnum):
    true = auto()
    false = auto()


class TestStrEnum:
    @given(truth=sampled_from(Truth))
    def test_main(self, truth: Truth) -> None:
        assert isinstance(truth, str)
        assert truth == truth.name
