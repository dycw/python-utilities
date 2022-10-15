from enum import Enum
from typing import Any
from typing import TypeVar

from beartype import beartype


class StrEnum(str, Enum):
    """An enum whose elements are themselves strings."""

    @staticmethod
    @beartype
    def _generate_next_value_(
        name: str, start: Any, count: int, last_values: Any
    ) -> str:
        _ = start, count, last_values
        return name


_E = TypeVar("_E", bound=Enum)


@beartype
def parse_enum(enum: type[_E], x: _E | str, /) -> _E:
    """Parse a string into the enum."""

    return enum[x] if isinstance(x, str) else x
