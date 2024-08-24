from __future__ import annotations

from enum import Enum, StrEnum, auto

from hypothesis import given
from hypothesis.strategies import DataObject, data, lists, sampled_from
from pytest import mark, param, raises

from utilities.enum import (
    EnsureEnumError,
    MaybeStr,
    ParseEnumError,
    ensure_enum,
    parse_enum,
)


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


class TestParseEnum:
    @given(data=data())
    def test_generic_enum(self, *, data: DataObject) -> None:
        class Truth(Enum):
            true = auto()
            false = auto()

        truth: Truth = data.draw(sampled_from(Truth))
        name = truth.name
        input_ = data.draw(sampled_from([name, name.upper(), name.lower()]))
        result = parse_enum(input_, Truth)
        assert result is truth

    @given(data=data())
    def test_generic_enum_case_sensitive(self, *, data: DataObject) -> None:
        class Truth(Enum):
            true = auto()
            false = auto()

        truth: Truth = data.draw(sampled_from(Truth))
        result = parse_enum(truth.name, Truth, case_sensitive=True)
        assert result is truth

    def test_generic_enum_none(self) -> None:
        class Truth(Enum):
            true = auto()
            false = auto()

        result = parse_enum(None, Truth)
        assert result is None

    @given(data=data())
    def test_str_enum(self, *, data: DataObject) -> None:
        class Truth(StrEnum):
            both = "both"
            true_ = "_true"
            false_ = "_false"

        input_, expected = data.draw(
            sampled_from([
                ("both", Truth.both),
                ("true_", Truth.true_),
                ("_true", Truth.true_),
                ("false_", Truth.false_),
                ("_false", Truth.false_),
            ])
        )
        result = parse_enum(input_, Truth)
        assert result is expected

    @given(data=data())
    def test_generic_enum_error_duplicates(self, *, data: DataObject) -> None:
        class Example(Enum):
            member = auto()
            MEMBER = auto()

        member = data.draw(sampled_from(Example))
        with raises(
            ParseEnumError,
            match=r"Enum .* must not contain duplicates \(case insensitive\); got .*",
        ):
            _ = parse_enum(member.name, Example)

    @mark.parametrize(
        ("case_sensitive", "desc"),
        [param(True, "sensitive"), param(False, "insensitive")],
    )
    def test_generic_enum_error_empty(self, *, case_sensitive: bool, desc: str) -> None:
        class Truth(Enum):
            true = auto()
            false = auto()

        with raises(
            ParseEnumError, match=rf"Enum .* does not contain 'invalid' \(case {desc}\)"
        ):
            _ = parse_enum("invalid", Truth, case_sensitive=case_sensitive)

    @mark.parametrize(
        ("case_sensitive", "desc"),
        [param(True, "sensitive"), param(False, "insensitive")],
    )
    def test_str_enum_error_empty(self, *, case_sensitive: bool, desc: str) -> None:
        class Truth(StrEnum):
            true = "true"
            false = "false"

        with raises(
            ParseEnumError, match=rf"Enum .* does not contain 'invalid' \(case {desc}\)"
        ):
            _ = parse_enum("invalid", Truth, case_sensitive=case_sensitive)

    def test_str_enum_error_ambiguous(self) -> None:
        class Truth(StrEnum):
            true_or_false = "true"
            false = "TRUE_OR_FALSE"

        with raises(
            ParseEnumError,
            match=r"StrEnum .* contains 'true_or_false' in both its keys and values \(case insensitive\)",
        ):
            _ = parse_enum("true_or_false", Truth)

    def test_str_enum_error_case_sensitive_ambiguous(self) -> None:
        class Truth(StrEnum):
            true_or_false = "true"
            false = "true_or_false"

        with raises(
            ParseEnumError,
            match=r"StrEnum .* contains 'true_or_false' in both its keys and values \(case sensitive\)",
        ):
            _ = parse_enum("true_or_false", Truth, case_sensitive=True)
