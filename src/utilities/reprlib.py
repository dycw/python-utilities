from __future__ import annotations

import reprlib
from typing import Any

RICH_MAX_WIDTH: int = 80
RICH_INDENT_SIZE: int = 4
RICH_MAX_LENGTH: int | None = 20
RICH_MAX_STRING: int | None = None
RICH_MAX_DEPTH: int | None = None
RICH_EXPAND_ALL: bool = False


def get_repr(
    obj: Any,
    /,
    *,
    max_width: int = RICH_MAX_WIDTH,
    indent_size: int = RICH_INDENT_SIZE,
    max_length: int | None = RICH_MAX_LENGTH,
    max_string: int | None = RICH_MAX_STRING,
    max_depth: int | None = RICH_MAX_DEPTH,
    expand_all: bool = RICH_EXPAND_ALL,
) -> str:
    """Get the representation of an object."""
    try:
        from rich.pretty import pretty_repr
    except ModuleNotFoundError:  # pragma: no cover
        return reprlib.repr(obj)
    return pretty_repr(
        obj,
        max_width=max_width,
        indent_size=indent_size,
        max_length=max_length,
        max_string=max_string,
        max_depth=max_depth,
        expand_all=expand_all,
    )


def get_repr_and_class(
    obj: Any,
    /,
    *,
    max_width: int = RICH_MAX_WIDTH,
    indent_size: int = RICH_INDENT_SIZE,
    max_length: int | None = RICH_MAX_LENGTH,
    max_string: int | None = RICH_MAX_STRING,
    max_depth: int | None = RICH_MAX_DEPTH,
    expand_all: bool = RICH_EXPAND_ALL,
) -> str:
    """Get the `reprlib`-representation & class of an object."""
    repr_use = get_repr(
        obj,
        max_width=max_width,
        indent_size=indent_size,
        max_length=max_length,
        max_string=max_string,
        max_depth=max_depth,
        expand_all=expand_all,
    )
    return f"Object {repr_use!r} of type {type(obj).__name__!r}"


__all__ = [
    "RICH_EXPAND_ALL",
    "RICH_INDENT_SIZE",
    "RICH_MAX_DEPTH",
    "RICH_MAX_LENGTH",
    "RICH_MAX_STRING",
    "RICH_MAX_WIDTH",
    "get_repr",
    "get_repr_and_class",
]
