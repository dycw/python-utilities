from enum import auto

from hypothesis import given
from hypothesis.strategies import DataObject, data, sampled_from

from utilities.enum import StrEnum, parse_enum


class Truth(StrEnum):
    true = auto()
    false = auto()


class TestParseEnum:
    @given(data=data(), truth=sampled_from(Truth))
    def test_main(self, data: DataObject, truth: Truth) -> None:
        input_ = data.draw(sampled_from([truth, truth.name]))
        result = parse_enum(Truth, input_)
        assert result is truth


class TestStrEnum:
    @given(truth=sampled_from(Truth))
    def test_main(self, truth: Truth) -> None:
        assert isinstance(truth, str)
        assert truth == truth.name
