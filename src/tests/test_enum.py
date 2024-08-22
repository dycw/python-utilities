from __future__ import annotations

from enum import Enum, auto

from hypothesis import given
from hypothesis.strategies import DataObject, data, lists, sampled_from
from pytest import raises

from utilities.enum import (
    EnsureEnumError,
    MaybeStr,
    ParseEnumError,
    ensure_enum,
    parse_enum,
)


class TestParseEnum:
    @given(data=data())
    def test_main(self, *, data: DataObject) -> None:
        class Truth(Enum):
            true = auto()
            false = auto()

        truth = data.draw(sampled_from(Truth))
        name = truth.name
        input_ = data.draw(sampled_from([name, name.upper(), name.lower()]))
        result = parse_enum(input_, Truth, case_sensitive=False)
        assert result is truth

    @given(data=data())
    def test_case_sensitive(self, *, data: DataObject) -> None:
        class Truth(Enum):
            true = auto()
            false = auto()

        truth = data.draw(sampled_from(Truth))
        result = parse_enum(truth.name, Truth, case_sensitive=True)
        assert result is truth

    def test_error_case_sensitive_empty(self) -> None:
        class Example(Enum):
            true = auto()
            false = auto()

        with raises(ParseEnumError, match=r"Enum .* does not contain 'invalid'"):
            _ = parse_enum("invalid", Example, case_sensitive=True)

    @given(data=data())
    def test_error_bijection(self, *, data: DataObject) -> None:
        class Example(Enum):
            member = auto()
            MEMBER = auto()

        member = data.draw(sampled_from(Example))
        with raises(
            ParseEnumError,
            match=r"Enum .* must not contain duplicates \(case insensitive\); got .*\.",
        ):
            _ = parse_enum(member.name, Example)

    def test_error_case_insensitive_empty(self) -> None:
        class Example(Enum):
            true = auto()
            false = auto()

        with raises(
            ParseEnumError,
            match=r"Enum .* does not contain 'invalid' \(case insensitive\)\.",
        ):
            _ = parse_enum("invalid", Example)

    def test_none(self) -> None:
        class Truth(Enum):
            true = auto()
            false = auto()

        result = parse_enum(None, Truth)
        assert result is None


class TestEnsureEnum:
    @given(data=data())
    def test_single_value_single_enum(self, *, data: DataObject) -> None:
        class Truth(Enum):
            true = auto()
            false = auto()

        truth: Truth = data.draw(sampled_from(Truth))
        input_: MaybeStr[Truth] = data.draw(sampled_from([truth, truth.name]))
        result = ensure_enum(input_, Truth)
        assert result is truth

    @given(data=data())
    def test_iterable_value_single_enum(self, *, data: DataObject) -> None:
        class Truth(Enum):
            true = auto()
            false = auto()

        truth: Truth = data.draw(sampled_from(Truth))
        input_: list[MaybeStr[Truth]] = data.draw(
            lists(sampled_from([truth, truth.name]))
        )
        result = list(ensure_enum(input_, Truth))
        for r in result:
            assert r is truth

    @given(data=data())
    def test_single_value_multiple_enums(self, *, data: DataObject) -> None:
        class Truth1(Enum):
            true1 = auto()
            false1 = auto()

        class Truth2(Enum):
            true2 = auto()
            false2 = auto()

        truth: Truth1 | Truth2 = data.draw(sampled_from(Truth1) | sampled_from(Truth2))
        input_: MaybeStr[Truth1 | Truth2] = data.draw(sampled_from([truth, truth.name]))
        result = ensure_enum(input_, (Truth1, Truth2))
        assert result is truth

    @given(data=data())
    def test_multiple_values_multiple_enums(self, *, data: DataObject) -> None:
        class Truth1(Enum):
            true1 = auto()
            false1 = auto()

        class Truth2(Enum):
            true2 = auto()
            false2 = auto()

        truth: Truth1 | Truth2 = data.draw(sampled_from(Truth1) | sampled_from(Truth2))
        input_: list[MaybeStr[Truth1 | Truth2]] = data.draw(
            lists(sampled_from([truth, truth.name]))
        )
        result = list(ensure_enum(input_, (Truth1, Truth2)))
        for r in result:
            assert r is truth

    def test_none(self) -> None:
        class Truth(Enum):
            true = auto()
            false = auto()

        result = ensure_enum(None, Truth)
        assert result is None

    @given(data=data())
    def test_error_single_value_single_enum(self, *, data: DataObject) -> None:
        class Truth1(Enum):
            true1 = auto()
            false1 = auto()

        class Truth2(Enum):
            true2 = auto()
            false2 = auto()

        truth: Truth1 = data.draw(sampled_from(Truth1))
        with raises(EnsureEnumError, match=".* is not an instance of .*"):
            _ = ensure_enum(truth, Truth2)

    @given(data=data())
    def test_error_single_value_multiple_enums(self, *, data: DataObject) -> None:
        class Truth1(Enum):
            true1 = auto()
            false1 = auto()

        class Truth2(Enum):
            true2 = auto()
            false2 = auto()

        truth: Truth1 = data.draw(sampled_from(Truth1))
        with raises(EnsureEnumError, match=".* is not an instance of .*"):
            _ = ensure_enum(truth, (Truth2,))
