from __future__ import annotations

from collections.abc import Iterable
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
    enum_ = cast(Iterable[Any], enum)
    if case_sensitive:
        els = {el for el in enum_ if el.name == member}
    else:
        els = {el for el in enum_ if el.name.casefold() == member.casefold()}
    try:
        (el,) = els
    except ValueError:
        raise ParseEnumError(enum=enum, member=member) from None
    else:
        return el


@dataclass(kw_only=True)
class ParseEnumError(Exception):
    enum: type[Enum]
    member: str

    @override
    def __str__(self) -> str:
        return f"Enum {self.enum} does not contain {self.member!r}."


__all__ = ["ParseEnumError", "StrEnum", "ensure_enum", "parse_enum"]
