from collections.abc import Iterable
from enum import Enum
from typing import Any, TypeVar, Union, cast

from beartype import beartype

from utilities.sys import PYTHON_AT_LEAST_3_11

if PYTHON_AT_LEAST_3_11:  # pragma: py-le-310
    from enum import StrEnum as _StrEnum  # type:ignore[]  # pragma: py-ne-310
else:  # pragma: py-ge-311

    class _StrEnum(str, Enum):
        """An enum whose elements are themselves strings."""

        @staticmethod
        @beartype
        def _generate_next_value_(
            name: str, start: Any, count: int, last_values: Any
        ) -> str:
            _ = start, count, last_values
            return name


StrEnum = _StrEnum


_E = TypeVar("_E", bound=Enum)


@beartype
def parse_enum(enum: type[_E], member: str, /, *, case_sensitive: bool = True) -> _E:
    """Parse a string into the enum."""
    enum_ = cast(Iterable[Any], enum)
    if case_sensitive:
        els = {el for el in enum_ if el.name == member}
    else:
        els = {el for el in enum_ if el.name.lower() == member.lower()}
    if (n := len(els)) == 0:
        msg = f"{enum=}, {member=}"
        raise NoMatchingMemberError(msg)
    if n == 1:
        (el,) = els
        return el
    msg = f"{enum=}, {member=}"
    raise MultipleMatchingMembersError(msg)


class NoMatchingMemberError(Exception):
    """Raised when an iterable is empty."""


class MultipleMatchingMembersError(Exception):
    """Raised when an iterable contains multiple elements."""


@beartype
def ensure_enum(
    enum: type[_E], member: Union[_E, str], /, *, case_sensitive: bool = True
) -> _E:
    """Ensure the object is a member of the enum."""
    if isinstance(member, Enum):
        return member
    return parse_enum(enum, member, case_sensitive=case_sensitive)
