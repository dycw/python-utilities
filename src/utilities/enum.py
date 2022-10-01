from enum import Enum
from typing import Any

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
