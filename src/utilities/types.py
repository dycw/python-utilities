from typing import Any, Union

from beartype import beartype

Number = Union[int, float]
NoneType = type(None)


@beartype
def ensure_class(x: Any, /) -> type[Any]:
    """Ensure the class of an object is returned, if it is not a class."""
    return x if isinstance(x, type) else type(x)


@beartype
def issubclass_except_bool_int(x: type[Any], y: type[Any], /) -> bool:
    """Checks for the subclass relation, except bool < int."""
    return issubclass(x, y) and not (issubclass(x, bool) and issubclass(int, y))
