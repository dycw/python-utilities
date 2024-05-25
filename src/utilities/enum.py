from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Any, Generic, TypeVar

from typing_extensions import override

from utilities.iterables import (
    _OneStrCaseInsensitiveBijectionError,
    _OneStrCaseInsensitiveEmptyError,
    _OneStrCaseSensitiveEmptyError,
    one_str,
)

if TYPE_CHECKING:
    from collections.abc import Mapping

try:
    from enum import StrEnum as _StrEnum  # type: ignore[]
except ImportError:  # pragma: version-ge-311

    class _StrEnum(str, Enum):
        """An enum whose elements are themselves strings."""

        @staticmethod
        @override
        def _generate_next_value_(
            name: str, start: Any, count: int, last_values: Any
        ) -> str:
            _ = start, count, last_values
            return name


StrEnum = _StrEnum
_E = TypeVar("_E", bound=Enum)


def ensure_enum(
    enum: type[_E], member: _E | str, /, *, case_sensitive: bool = True
) -> _E:
    """Ensure the object is a member of the enum."""
    if isinstance(member, Enum):
        return member
    return parse_enum(enum, member, case_sensitive=case_sensitive)


def parse_enum(enum: type[_E], member: str, /, *, case_sensitive: bool = True) -> _E:
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


__all__ = ["ParseEnumError", "StrEnum", "ensure_enum", "parse_enum"]
