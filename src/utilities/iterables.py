from __future__ import annotations

from collections import Counter
from collections.abc import Hashable, Iterable
from typing import Any


def check_duplicates(x: Iterable[Hashable], /) -> None:
    """Check if an iterable contains any duplicates."""
    dup = {k: v for k, v in Counter(x).items() if v > 1}
    if len(dup) >= 1:
        msg = f"{dup=}"
        raise IterableContainsDuplicatesError(msg)


class IterableContainsDuplicatesError(ValueError):
    """Raised when an iterable contains duplicates."""


def is_iterable_not_str(x: Any, /) -> bool:
    """Check if an object is iterable, but not a string."""
    try:
        iter(x)
    except TypeError:
        return False
    return not isinstance(x, str)
