from typing import Any

from beartype import beartype


@beartype
def get_class_name(x: Any, /) -> str:
    """Get the name of a class."""
    return (x if isinstance(x, type) else type(x)).__name__
