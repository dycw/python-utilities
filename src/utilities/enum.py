from __future__ import annotations

from collections.abc import Iterable
from contextlib import suppress
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Any, Generic, TypeVar, cast, overload

from typing_extensions import override

from utilities.iterables import (
    _OneStrCaseInsensitiveBijectionError,
    _OneStrCaseInsensitiveEmptyError,
    _OneStrCaseSensitiveEmptyError,
    is_iterable_not_enum,
    is_iterable_not_str,
    one_str,
)

if TYPE_CHECKING:
    from collections.abc import Mapping


_E = TypeVar("_E", bound=Enum)
_E1 = TypeVar("_E1", bound=Enum)
_E2 = TypeVar("_E2", bound=Enum)
MaybeStr = _E | str


@overload
def ensure_enum(
    value_or_values: None,
    enum_or_enums: type[_E | (_E1 | _E2)],
    /,
    *,
    case_sensitive: bool = ...,
) -> None: ...
@overload
def ensure_enum(
    value_or_values: MaybeStr[_E],
    enum_or_enums: type[_E],
    /,
    *,
    case_sensitive: bool = ...,
) -> _E: ...
@overload
def ensure_enum(
    value_or_values: Iterable[MaybeStr[_E]],
    enum_or_enums: type[_E],
    /,
    *,
    case_sensitive: bool = ...,
) -> Iterable[_E]: ...
@overload
def ensure_enum(
    value_or_values: MaybeStr[_E1 | _E2],
    enum_or_enums: tuple[type[_E1], type[_E2]],
    /,
    *,
    case_sensitive: bool = ...,
) -> _E1 | _E2: ...
@overload
def ensure_enum(
    value_or_values: Iterable[MaybeStr[_E1 | _E2]],
    enum_or_enums: tuple[type[_E1], type[_E2]],
    /,
    *,
    case_sensitive: bool = ...,
) -> Iterable[_E1 | _E2]: ...
def ensure_enum(
    value_or_values: Any, enum_or_enums: Any, /, *, case_sensitive: bool = False
) -> Any:
    """Ensure the object is a member of the enum."""
    if value_or_values is None:
        return None
    if is_iterable_not_str(value_or_values):
        values = cast(Iterable[MaybeStr[Enum]], value_or_values)
        return (
            ensure_enum(v, enum_or_enums, case_sensitive=case_sensitive) for v in values
        )
    value = cast(MaybeStr[Enum], value_or_values)
    if is_iterable_not_enum(enum_or_enums):
        enums = cast(tuple[type[Enum], ...], enum_or_enums)
        for enum in enums:
            with suppress(_EnsureEnumSingleValueSingleEnumError):
                return ensure_enum(value, enum, case_sensitive=case_sensitive)
        raise _EnsureEnumSingleValueMultipleEnumsError(
            value=value, enums=enums, case_sensitive=case_sensitive
        )
    enum = cast(type[Enum], enum_or_enums)
    if isinstance(value, Enum):
        if isinstance(value, enum):
            return value
        raise _EnsureEnumSingleValueSingleEnumError(
            value=value, enum=enum, case_sensitive=case_sensitive
        )
    try:
        return parse_enum(enum, value, case_sensitive=case_sensitive)
    except ParseEnumError:
        raise _EnsureEnumSingleValueSingleEnumError(
            value=value, enum=enum, case_sensitive=case_sensitive
        ) from None


@dataclass(kw_only=True)
class EnsureEnumError(Exception): ...


@dataclass(kw_only=True)
class _EnsureEnumSingleValueSingleEnumError(EnsureEnumError):
    value: Any
    enum: type[Enum]
    case_sensitive: bool

    @override
    def __str__(self) -> str:
        return f"Value {self.value} is not an instance of {self.enum}"


@dataclass(kw_only=True)
class _EnsureEnumSingleValueMultipleEnumsError(EnsureEnumError):
    value: Any
    enums: tuple[type[Enum], ...]
    case_sensitive: bool

    @override
    def __str__(self) -> str:
        return f"Value {self.value} is not an instance of any of {self.enums}"


# _EnsureEnumSingleValueSingleEnumError


def parse_enum(enum: type[_E], member: str, /, *, case_sensitive: bool = False) -> _E:
    """Parse a string into the enum."""
    names = {e.name for e in enum}
    try:
        match = one_str(names, member, case_sensitive=case_sensitive)
    except _OneStrCaseSensitiveEmptyError:
        raise _ParseEnumCaseSensitiveEmptyError(enum=enum, member=member) from None
    except _OneStrCaseInsensitiveBijectionError as error:
        raise _ParseEnumCaseInsensitiveBijectionError(
            enum=enum, member=member, counts=error.counts
        ) from None
    except _OneStrCaseInsensitiveEmptyError:
        raise _ParseEnumCaseInsensitiveEmptyError(enum=enum, member=member) from None
    return enum[match]


@dataclass(kw_only=True)
class ParseEnumError(Exception, Generic[_E]):
    enum: type[_E]
    member: str


@dataclass(kw_only=True)
class _ParseEnumCaseSensitiveEmptyError(ParseEnumError):
    @override
    def __str__(self) -> str:
        return f"Enum {self.enum} does not contain {self.member!r}."


@dataclass(kw_only=True)
class _ParseEnumCaseInsensitiveBijectionError(ParseEnumError):
    counts: Mapping[str, int]

    @override
    def __str__(self) -> str:
        return f"Enum {self.enum} must not contain duplicates (case insensitive); got {self.counts}."


@dataclass(kw_only=True)
class _ParseEnumCaseInsensitiveEmptyError(ParseEnumError):
    @override
    def __str__(self) -> str:
        return f"Enum {self.enum} does not contain {self.member!r} (case insensitive)."


__all__ = ["EnsureEnumError", "MaybeStr", "ParseEnumError", "ensure_enum", "parse_enum"]
