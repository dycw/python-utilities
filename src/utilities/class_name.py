from typing import Any

from beartype import beartype

from utilities.text import snake_case


@beartype
def get_class_name(x: Any, /, *, snake: bool = False) -> str:
    """Get the name of a class."""
    cls_name = (x if isinstance(x, type) else type(x)).__name__
    return snake_case(cls_name) if snake else cls_name
