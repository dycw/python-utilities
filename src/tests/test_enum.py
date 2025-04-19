from __future__ import annotations

from enum import Enum, StrEnum, auto
from typing import TYPE_CHECKING, Literal

from hypothesis import given
from hypothesis.strategies import DataObject, data, sampled_from
from pytest import raises

from utilities.enum import (
    _EnsureEnumParseError,
    _EnsureEnumTypeEnumError,
    _ParseEnumByKindNonUniqueError,
    _ParseEnumGenericEnumEmptyError,
    _ParseEnumStrEnumEmptyError,
    _ParseEnumStrEnumNonUniqueError,
    ensure_enum,
    parse_enum,
)

if TYPE_CHECKING:
    from utilities.types import EnumLike


class TestParseEnum:
    @given(data=data())
    def test_generic_enum_case_insensitive(self, *, data: DataObject) -> None:
        class Truth(Enum):
            true = auto()
            false = auto()

        input_, expected = data.draw(
            sampled_from([
                ("true", Truth.true),
                ("TRUE", Truth.true),
                ("false", Truth.false),
                ("FALSE", Truth.false),
            ])
        )
        result = parse_enum(input_, Truth)
        assert result is expected

    @given(data=data())
    def test_generic_enum_case_sensitive(self, *, data: DataObject) -> None:
        class Truth(Enum):
            true = auto()
            false = auto()

        input_, expected = data.draw(
            sampled_from([("true", Truth.true), ("false", Truth.false)])
        )
        result = parse_enum(input_, Truth, case_sensitive=True)
        assert result is expected

    @given(data=data())
    def test_str_enum_case_insensitive(self, *, data: DataObject) -> None:
        class Truth(StrEnum):
            both = "both"
            true_by_name = "true_by_value"
            false_by_name = "false_by_value"

        input_, expected = data.draw(
            sampled_from([
                ("both", Truth.both),
                ("true_by_name", Truth.true_by_name),
                ("true_by_value", Truth.true_by_name),
                ("false_by_name", Truth.false_by_name),
                ("false_by_value", Truth.false_by_name),
            ])
        )
        input_ = data.draw(sampled_from([input_, input_.upper()]))
        result = parse_enum(input_, Truth)
        assert result is expected

    @given(data=data())
    def test_str_enum_case_sensitive(self, *, data: DataObject) -> None:
        class Truth(StrEnum):
            both = "both"
            true_by_name = "true_by_value"
            false_by_name = "false_by_value"

        input_, expected = data.draw(
            sampled_from([
                ("both", Truth.both),
                ("true_by_name", Truth.true_by_name),
                ("true_by_value", Truth.true_by_name),
                ("false_by_name", Truth.false_by_name),
                ("false_by_value", Truth.false_by_name),
            ])
        )
        result = parse_enum(input_, Truth, case_sensitive=True)
        assert result is expected

    def test_error_generic_enum_case_insensitive_empty(self) -> None:
        class Truth(Enum):
            true = auto()
            false = auto()

        with raises(
            _ParseEnumGenericEnumEmptyError,
            match=r"^Enum 'Truth' member names do not contain 'invalid' \(modulo case\)",
        ):
            _ = parse_enum("invalid", Truth)

    @given(input_=sampled_from(["x", "X"]))
    def test_error_generic_enum_case_insensitive_non_unique(
        self, *, input_: Literal["x", "X"]
    ) -> None:
        class Example(Enum):
            x = auto()
            X = auto()

        with raises(
            _ParseEnumByKindNonUniqueError,
            match=r"^Enum 'Example' member names must contain '[xX]' exactly once \(modulo case\); got 'x', 'X' and perhaps more",
        ):
            _ = parse_enum(input_, Example)

    def test_error_generic_enum_case_sensitive_empty(self) -> None:
        class Truth(Enum):
            true = auto()
            false = auto()

        with raises(
            _ParseEnumGenericEnumEmptyError,
            match=r"^Enum 'Truth' member names do not contain 'invalid'",
        ):
            _ = parse_enum("invalid", Truth, case_sensitive=True)

    def test_error_str_enum_case_insensitive_empty(self) -> None:
        class Truth(StrEnum):
            true = "TRUE"
            false = "FALSE"

        with raises(
            _ParseEnumStrEnumEmptyError,
            match=r"^StrEnum 'Truth' member names and values do not contain 'invalid' \(modulo case\)",
        ):
            _ = parse_enum("invalid", Truth)

    @given(data=data(), input_=sampled_from(["true", "false"]))
    def test_error_str_enum_case_insensitive_non_unique(
        self, *, data: DataObject, input_: Literal["true", "false"]
    ) -> None:
        class Truth(StrEnum):
            true = "FALSE"
            false = "TRUE"

        input_use = data.draw(sampled_from([input_, input_.upper()]))
        with raises(
            _ParseEnumStrEnumNonUniqueError,
            match=r"^StrEnum 'Truth' member names and values must contain '(true|false|TRUE|FALSE)' exactly once \(modulo case\); got '(true|false)' by name and '(true|false)' by value",
        ):
            _ = parse_enum(input_use, Truth)

    def test_error_str_enum_case_sensitive_empty(self) -> None:
        class Truth(StrEnum):
            true = "true"
            false = "false"

        with raises(
            _ParseEnumStrEnumEmptyError,
            match=r"^StrEnum 'Truth' member names and values do not contain 'invalid'",
        ):
            _ = parse_enum("invalid", Truth, case_sensitive=True)

    @given(input_=sampled_from(["true", "false"]))
    def test_error_str_enum_case_sensitive_non_unique(
        self, *, input_: Literal["true", "false"]
    ) -> None:
        class Truth(StrEnum):
            true = "false"
            false = "true"

        with raises(
            _ParseEnumStrEnumNonUniqueError,
            match=r"^StrEnum 'Truth' member names and values must contain '(true|false)' exactly once; got '(true|false)' by name and '(true|false)' by value",
        ):
            _ = parse_enum(input_, Truth, case_sensitive=True)

    @given(data=data())
    def test_ensure(self, *, data: DataObject) -> None:
        class Truth(Enum):
            true = auto()
            false = auto()

        truth: Truth = data.draw(sampled_from(Truth))
        input_: EnumLike[Truth] = data.draw(sampled_from([truth, truth.name]))
        result = ensure_enum(input_, Truth)
        assert result is truth

    def test_ensure_none(self) -> None:
        class Truth(Enum):
            true = auto()
            false = auto()

        result = ensure_enum(None, Truth)
        assert result is None

    @given(data=data())
    def test_error_ensure_type(self, *, data: DataObject) -> None:
        class Truth1(Enum):
            true1 = auto()
            false1 = auto()

        class Truth2(Enum):
            true2 = auto()
            false2 = auto()

        truth: Truth1 = data.draw(sampled_from(Truth1))
        with raises(_EnsureEnumTypeEnumError, match=".* is not an instance of .*"):
            _ = ensure_enum(truth, Truth2)

    def test_error_ensure_parse(self) -> None:
        class Truth(Enum):
            true = auto()
            false = auto()

        with raises(
            _EnsureEnumParseError, match="Unable to ensure enum; got 'invalid'"
        ):
            _ = ensure_enum("invalid", Truth)
