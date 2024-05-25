from __future__ import annotations

from enum import Enum, auto
from typing import cast

from hypothesis import given
from hypothesis.strategies import DataObject, data, sampled_from
from pytest import raises

from utilities.enum import ParseEnumError, StrEnum, ensure_enum, parse_enum


class TestParseEnum:
    @given(data=data())
    def test_case_sensitive(self, *, data: DataObject) -> None:
        class Truth(Enum):
            true = auto()
            false = auto()

        truth = data.draw(sampled_from(Truth))
        result = parse_enum(Truth, truth.name)
        assert result is truth

    @given(data=data())
    def test_case_insensitive(self, *, data: DataObject) -> None:
        class Truth(Enum):
            true = auto()
            false = auto()

        truth = data.draw(sampled_from(Truth))
        name = truth.name
        input_ = data.draw(sampled_from([name, name.upper(), name.lower()]))
        result = parse_enum(Truth, input_, case_sensitive=False)
        assert result is truth

    def test_error_case_sensitive_empty(self) -> None:
        class Example(Enum):
            true = auto()
            false = auto()

        with raises(ParseEnumError, match=r"Enum .* does not contain 'invalid'"):
            _ = parse_enum(Example, "invalid")

    @given(data=data())
    def test_error_bijection_error(self, *, data: DataObject) -> None:
        class Example(Enum):
            member = auto()
            MEMBER = auto()

        member = data.draw(sampled_from(Example))
        with raises(
            ParseEnumError,
            match=r"Enum .* must not contain duplicates \(case insensitive\); got .*\.",
        ):
            _ = parse_enum(Example, member.name, case_sensitive=False)

    def test_error_case_insensitive_empty_error(self) -> None:
        class Example(Enum):
            true = auto()
            false = auto()

        with raises(
            ParseEnumError,
            match=r"Enum .* does not contain 'invalid' \(case insensitive\)\.",
        ):
            _ = parse_enum(Example, "invalid", case_sensitive=False)


class TestEnsureEnum:
    @given(data=data())
    def test_main(self, *, data: DataObject) -> None:
        class Truth(Enum):
            true = auto()
            false = auto()

        truth = data.draw(sampled_from(Truth))
        input_ = data.draw(sampled_from([truth, truth.name]))
        result = ensure_enum(Truth, input_)
        assert result is truth


class TestStrEnum:
    @given(data=data())
    def test_main(self, *, data: DataObject) -> None:
        class Truth(cast(type[Enum], StrEnum)):
            true = auto()
            false = auto()

        truth = data.draw(sampled_from(Truth))
        assert isinstance(truth, Enum)
        assert isinstance(truth, str)
        assert truth == truth.name
