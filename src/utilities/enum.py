from enum import Enum
from typing import Any, TypeVar, Union

from beartype import beartype


class StrEnum(str, Enum):
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


_E = TypeVar("_E", bound=Enum)


@beartype
def parse_enum(enum: type[_E], member: Union[_E, str], /) -> _E:
    """Parse a string into the enum."""
    return enum[member] if isinstance(member, str) else member
