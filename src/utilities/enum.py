from __future__ import annotations

from dataclasses import dataclass
from enum import Enum, StrEnum
from typing import TYPE_CHECKING, Literal, assert_never, overload, override

from utilities.functions import ensure_str
from utilities.iterables import OneStrEmptyError, OneStrNonUniqueError, one_str

if TYPE_CHECKING:
    from utilities.types import EnumLike


##


@overload
def ensure_enum[E: Enum](
    value: None, enum: type[E], /, *, case_sensitive: bool = False
) -> None: ...
@overload
def ensure_enum[E: Enum](
    value: EnumLike[E], enum: type[E], /, *, case_sensitive: bool = False
) -> E: ...
def ensure_enum[E: Enum](
    value: EnumLike[E] | None, enum: type[E], /, *, case_sensitive: bool = False
) -> E | None:
    """Ensure the object is a member of the enum."""
    if value is None:
        return None
    if isinstance(value, enum):
        return value
    if isinstance(value, Enum):
        raise _EnsureEnumTypeEnumError(value=value, enum=enum)
    try:
        return parse_enum(value, enum, case_sensitive=case_sensitive)
    except ParseEnumError as error:
        raise _EnsureEnumParseError(value=error.value, enum=error.enum) from None


@dataclass(kw_only=True, slots=True)
class EnsureEnumError[E: Enum](Exception):
    value: EnumLike[E]
    enum: type[E]


@dataclass(kw_only=True, slots=True)
class _EnsureEnumTypeEnumError(EnsureEnumError):
    @override
    def __str__(self) -> str:
        return f"{self.value!r} is not an instance of {self.enum!r}"


@dataclass(kw_only=True, slots=True)
class _EnsureEnumParseError(EnsureEnumError):
    @override
    def __str__(self) -> str:
        return f"Unable to ensure enum; got {self.value!r}"


##


def parse_enum[E: Enum](
    value: str, enum: type[E], /, *, case_sensitive: bool = False
) -> E:
    """Parse a string into the enum."""
    by_name = _parse_enum_one(value, enum, "names", case_sensitive=case_sensitive)
    if not issubclass(enum, StrEnum):
        if by_name is not None:
            return by_name
        raise _ParseEnumGenericEnumEmptyError(
            value=value,
            enum=enum,
            case_sensitive=case_sensitive,
            names_or_values="names",
        )
    by_value = _parse_enum_one(value, enum, "values", case_sensitive=case_sensitive)
    match by_name, by_value:
        case None, None:
            raise _ParseEnumStrEnumEmptyError(
                value=value, enum=enum, case_sensitive=case_sensitive
            )
        case Enum(), None:
            return by_name
        case None, Enum():
            return by_value
        case Enum(), Enum():
            if by_name is by_value:
                return by_name
            raise _ParseEnumStrEnumNonUniqueError(
                value=value,
                enum=enum,
                case_sensitive=case_sensitive,
                by_name=by_name.name,
                by_value=by_value.name,
            )
        case _ as never:
            assert_never(never)


type _NamesOrValues = Literal["names", "values"]


def _parse_enum_one[E: Enum](
    value: str,
    enum: type[E],
    names_or_values: _NamesOrValues,
    /,
    *,
    case_sensitive: bool = False,
) -> E | None:
    """Pair one aspect of the enums."""
    match names_or_values:
        case "names":
            names = [e.name for e in enum]
        case "values":
            names = [ensure_str(e.value) for e in enum]
        case _ as never:
            assert_never(never)
    try:
        name = one_str(names, value, case_sensitive=case_sensitive)
    except OneStrEmptyError:
        return None
    except OneStrNonUniqueError as error:
        raise _ParseEnumByKindNonUniqueError(
            value=value,
            enum=enum,
            names_or_values=names_or_values,
            first=error.first,
            second=error.second,
        ) from None
    index = names.index(name)
    return list(enum)[index]


@dataclass(kw_only=True, slots=True)
class ParseEnumError[E: Enum](Exception):
    value: str
    enum: type[E]


@dataclass(kw_only=True, slots=True)
class _ParseEnumByKindNonUniqueError(ParseEnumError):
    names_or_values: _NamesOrValues
    first: str
    second: str

    @override
    def __str__(self) -> str:
        desc = "StrEnum" if issubclass(self.enum, StrEnum) else "Enum"
        return f"{desc} {self.enum.__name__!r} member {self.names_or_values} must contain {self.value!r} exactly once (modulo case); got {self.first!r}, {self.second!r} and perhaps more"


@dataclass(kw_only=True, slots=True)
class _ParseEnumGenericEnumEmptyError(ParseEnumError):
    names_or_values: _NamesOrValues
    case_sensitive: bool = False

    @override
    def __str__(self) -> str:
        desc = f"Enum {self.enum.__name__!r} member {self.names_or_values} do not contain {self.value!r}"
        return desc if self.case_sensitive else f"{desc} (modulo case)"


@dataclass(kw_only=True, slots=True)
class _ParseEnumStrEnumEmptyError(ParseEnumError):
    case_sensitive: bool = False

    @override
    def __str__(self) -> str:
        desc = f"StrEnum {self.enum.__name__!r} member names and values do not contain {self.value!r}"
        return desc if self.case_sensitive else f"{desc} (modulo case)"


@dataclass(kw_only=True, slots=True)
class _ParseEnumStrEnumNonUniqueError(ParseEnumError):
    case_sensitive: bool = False
    by_name: str
    by_value: str

    @override
    def __str__(self) -> str:
        head = f"StrEnum {self.enum.__name__!r} member names and values must contain {self.value!r} exactly once"
        mid = "" if self.case_sensitive else " (modulo case)"
        return (
            f"{head}{mid}; got {self.by_name!r} by name and {self.by_value!r} by value"
        )


__all__ = ["EnsureEnumError", "ParseEnumError", "ensure_enum", "parse_enum"]
