from typing import Any

from beartype import beartype


@beartype
def is_iterable_not_str(x: Any, /) -> bool:
    """Check if an object is iterable, but not a string."""

    try:
        iter(x)
    except TypeError:
        return False
    return not isinstance(x, str)
