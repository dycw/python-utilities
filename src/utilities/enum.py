from enum import Enum
from typing import Any, TypeVar, Union

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
            name: str,
            start: Any,
            count: int,
            last_values: Any,
        ) -> str:
            _ = start, count, last_values
            return name


StrEnum = _StrEnum


_E = TypeVar("_E", bound=Enum)


@beartype
def parse_enum(enum: type[_E], member: Union[_E, str], /) -> _E:
    """Parse a string into the enum."""
    return enum[member] if isinstance(member, str) else member
