from __future__ import annotations

import reprlib
from functools import partial
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Iterator

    from utilities.types import StrMapping

RICH_MAX_WIDTH: int = 80
RICH_INDENT_SIZE: int = 4
RICH_MAX_LENGTH: int | None = 20
RICH_MAX_STRING: int | None = None
RICH_MAX_DEPTH: int | None = None
RICH_EXPAND_ALL: bool = False


##


def get_call_args_mapping(*args: Any, **kwargs: Any) -> StrMapping:
    """Get the representation of a set of call arguments."""
    return {f"args[{i}]": v for i, v in enumerate(args)} | {
        f"kwargs[{k}]": v for k, v in kwargs.items()
    }


##


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


##


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


##


def yield_call_args_repr(
    *args: Any,
    _max_width: int = RICH_MAX_WIDTH,
    _indent_size: int = RICH_INDENT_SIZE,
    _max_length: int | None = RICH_MAX_LENGTH,
    _max_string: int | None = RICH_MAX_STRING,
    _max_depth: int | None = RICH_MAX_DEPTH,
    _expand_all: bool = RICH_EXPAND_ALL,
    **kwargs: Any,
) -> Iterator[str]:
    """Pretty print of a set of positional/keyword arguments."""
    mapping = get_call_args_mapping(*args, **kwargs)
    return yield_mapping_repr(
        mapping,
        _max_width=_max_width,
        _indent_size=_indent_size,
        _max_length=_max_length,
        _max_string=_max_string,
        _max_depth=_max_depth,
        _expand_all=_expand_all,
    )


##


def yield_mapping_repr(
    mapping: StrMapping,
    /,
    *,
    _max_width: int = RICH_MAX_WIDTH,
    _indent_size: int = RICH_INDENT_SIZE,
    _max_length: int | None = RICH_MAX_LENGTH,
    _max_string: int | None = RICH_MAX_STRING,
    _max_depth: int | None = RICH_MAX_DEPTH,
    _expand_all: bool = RICH_EXPAND_ALL,
) -> Iterator[str]:
    """Pretty print of a set of keyword arguments."""
    try:
        from rich.pretty import pretty_repr
    except ModuleNotFoundError:  # pragma: no cover
        repr_use = repr
    else:
        repr_use = partial(
            pretty_repr,
            max_width=_max_width,
            indent_size=_indent_size,
            max_length=_max_length,
            max_string=_max_string,
            max_depth=_max_depth,
            expand_all=_expand_all,
        )
    for k, v in mapping.items():
        yield f"{k} = {repr_use(v)}"


__all__ = [
    "RICH_EXPAND_ALL",
    "RICH_INDENT_SIZE",
    "RICH_MAX_DEPTH",
    "RICH_MAX_LENGTH",
    "RICH_MAX_STRING",
    "RICH_MAX_WIDTH",
    "get_call_args_mapping",
    "get_repr",
    "get_repr_and_class",
    "yield_call_args_repr",
    "yield_mapping_repr",
]
