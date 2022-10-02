from typing import Any

from beartype import beartype


@beartype
def ensure_str(x: Any, /) -> str:
    """Ensure an object is a string."""

    if isinstance(x, str):
        return x
    else:
        raise TypeError(f"{x=}")


class NotAString(TypeError):
    ...
