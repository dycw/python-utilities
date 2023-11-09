from __future__ import annotations

from collections.abc import Mapping
from collections.abc import Set as AbstractSet
from typing import Any, NoReturn

SequenceStrs = list[str] | tuple[str, ...]
IterableStrs = SequenceStrs | AbstractSet[str] | Mapping[str, Any]
Number = float | int


def never(x: NoReturn, /) -> NoReturn:
    """Never return. Used for exhaustive pattern matching."""
    msg = f'"never" was run with {x}'
    raise NeverError(msg)


class NeverError(Exception):
    """Raised when `never` is run."""


__all__ = [
    "IterableStrs",
    "never",
    "NeverError",
    "Number",
    "SequenceStrs",
]
