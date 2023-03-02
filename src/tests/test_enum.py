from enum import Enum, auto

from hypothesis import given
from hypothesis.strategies import DataObject, data, sampled_from
from pytest import raises

from utilities.enum import (
    MultipleMatchingMembersError,
    NoMatchingMemberError,
    StrEnum,
    ensure_enum,
    parse_enum,
)


class TestParseEnum:
    @given(data=data())
    def test_main(self, data: DataObject) -> None:
        class Truth(Enum):
            true = auto()
            false = auto()

        truth = data.draw(sampled_from(Truth))
        result = parse_enum(Truth, truth.name)
        assert result is truth

    @given(data=data())
    def test_case_insensitive(self, data: DataObject) -> None:
        class Truth(Enum):
            true = auto()
            false = auto()

        truth = data.draw(sampled_from(Truth))
        name = truth.name
        input_ = data.draw(sampled_from([name, name.upper(), name.lower()]))
        result = parse_enum(Truth, input_, case_sensitive=False)
        assert result is truth

    def test_no_matching_member(self) -> None:
        class Example(Enum):
            pass

        with raises(NoMatchingMemberError):
            _ = parse_enum(Example, "not-a-member")

    @given(data=data())
    def test_multiple_matching_members(self, data: DataObject) -> None:
        class Example(Enum):
            member = auto()
            MEMBER = auto()

        member = data.draw(sampled_from(Example))
        with raises(MultipleMatchingMembersError):
            _ = parse_enum(Example, member.name, case_sensitive=False)


class TestEnsureEnum:
    @given(data=data())
    def test_main(self, data: DataObject) -> None:
        class Truth(Enum):
            true = auto()
            false = auto()

        truth = data.draw(sampled_from(Truth))
        input_ = data.draw(sampled_from([truth, truth.name]))
        result = ensure_enum(Truth, input_)
        assert result is truth


class TestStrEnum:
    @given(data=data())
    def test_main(self, data: DataObject) -> None:
        class Truth(StrEnum):
            true = auto()
            false = auto()

        truth = data.draw(sampled_from(Truth))
        assert isinstance(truth, Enum)
        assert isinstance(truth, str)
        assert truth == truth.name
