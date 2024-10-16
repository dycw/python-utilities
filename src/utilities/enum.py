from __future__ import annotations

from dataclasses import dataclass
from enum import Enum, StrEnum
from typing import (
    TYPE_CHECKING,
    Generic,
    Literal,
    NoReturn,
    TypeVar,
    assert_never,
    overload,
)

from typing_extensions import override

from utilities.errors import ImpossibleCaseError
from utilities.iterables import (
    _OneStrCaseInsensitiveBijectionError,
    _OneStrCaseInsensitiveEmptyError,
    _OneStrCaseSensitiveEmptyError,
    one_str,
)
from utilities.text import ensure_str

if TYPE_CHECKING:
    from collections.abc import Mapping


_E = TypeVar("_E", bound=Enum)
MaybeStr = _E | str


@overload
def ensure_enum(
    value: None, enum: type[_E], /, *, case_sensitive: bool = ...
) -> None: ...
@overload
def ensure_enum(
    value: MaybeStr[_E], enum: type[_E], /, *, case_sensitive: bool = ...
) -> _E: ...
def ensure_enum(
    value: MaybeStr[_E] | None, enum: type[_E], /, *, case_sensitive: bool = False
) -> _E | None:
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
class EnsureEnumError(Exception, Generic[_E]):
    value: MaybeStr[_E]
    enum: type[_E]


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


def parse_enum(
    value: str, enum: type[_E], /, *, case_sensitive: bool = False
) -> _E | None:
    """Parse a string into the enum."""
    by_name = _parse_enum_by_kind(value, enum, "name", case_sensitive=case_sensitive)
    if not issubclass(enum, StrEnum):
        if by_name is not None:
            return by_name
        _parse_enum_raise_empty_error(value, enum, case_sensitive=case_sensitive)
    by_value = _parse_enum_by_kind(value, enum, "value", case_sensitive=case_sensitive)
    if (by_name is None) and (by_value is None):
        _parse_enum_raise_empty_error(value, enum, case_sensitive=case_sensitive)
    if (by_name is not None) and (by_value is None):
        return by_name
    if (by_name is None) and (by_value is not None):
        return by_value
    if (by_name is not None) and (by_value is not None):
        if by_name is by_value:
            return by_name
        if case_sensitive:
            raise _ParseEnumStrEnumCaseSensitiveAmbiguousError(value=value, enum=enum)
        raise _ParseEnumStrEnumCaseInsensitiveAmbiguousError(value=value, enum=enum)
    raise ImpossibleCaseError(case=[f"{by_name=}", f"{by_value=}"])  # pragma: no cover


def _parse_enum_by_kind(
    value: str,
    enum: type[_E],
    kind: Literal["name", "value"],
    /,
    *,
    case_sensitive: bool = False,
) -> _E | None:
    """Pair one aspect of the enums."""
    match kind:
        case "name":
            names = [e.name for e in enum]
        case "value":
            names = [ensure_str(e.value) for e in enum]
        case _ as never:  # pyright: ignore[reportUnnecessaryComparison]
            assert_never(never)
    try:
        name = one_str(names, value, case_sensitive=case_sensitive)
    except (_OneStrCaseSensitiveEmptyError, _OneStrCaseInsensitiveEmptyError):
        return None
    except _OneStrCaseInsensitiveBijectionError as error:
        raise _ParseEnumCaseInsensitiveBijectionError(
            value=value, enum=enum, counts=error.counts
        ) from None
    index = names.index(name)
    return list(enum)[index]


def _parse_enum_raise_empty_error(
    value: str, enum: type[_E], /, *, case_sensitive: bool = False
) -> NoReturn:
    """Raise the empty error."""
    if case_sensitive:
        raise _ParseEnumCaseSensitiveEmptyError(value=value, enum=enum)
    raise _ParseEnumCaseInsensitiveEmptyError(value=value, enum=enum)


@dataclass(kw_only=True, slots=True)
class ParseEnumError(Exception, Generic[_E]):
    value: str
    enum: type[_E]


@dataclass(kw_only=True, slots=True)
class _ParseEnumCaseInsensitiveBijectionError(ParseEnumError):
    counts: Mapping[str, int]

    @override
    def __str__(self) -> str:
        return f"Enum {self.enum} must not contain duplicates (case insensitive); got {self.counts}"


@dataclass(kw_only=True, slots=True)
class _ParseEnumCaseSensitiveEmptyError(ParseEnumError):
    @override
    def __str__(self) -> str:
        return f"Enum {self.enum} does not contain {self.value!r} (case sensitive)"


@dataclass(kw_only=True, slots=True)
class _ParseEnumCaseInsensitiveEmptyError(ParseEnumError):
    @override
    def __str__(self) -> str:
        return f"Enum {self.enum} does not contain {self.value!r} (case insensitive)"


@dataclass(kw_only=True, slots=True)
class _ParseEnumStrEnumCaseSensitiveAmbiguousError(ParseEnumError):
    @override
    def __str__(self) -> str:
        return f"StrEnum {self.enum} contains {self.value!r} in both its keys and values (case sensitive)"


@dataclass(kw_only=True, slots=True)
class _ParseEnumStrEnumCaseInsensitiveAmbiguousError(ParseEnumError):
    @override
    def __str__(self) -> str:
        return f"StrEnum {self.enum} contains {self.value!r} in both its keys and values (case insensitive)"


__all__ = ["EnsureEnumError", "MaybeStr", "ParseEnumError", "ensure_enum", "parse_enum"]
