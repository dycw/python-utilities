from __future__ import annotations

from typing import TYPE_CHECKING, Any

from rich.pretty import pretty_repr

from utilities.reprlib import (
    RICH_EXPAND_ALL,
    RICH_INDENT_SIZE,
    RICH_MAX_DEPTH,
    RICH_MAX_LENGTH,
    RICH_MAX_STRING,
    RICH_MAX_WIDTH,
)

if TYPE_CHECKING:
    from collections.abc import Iterator

MAX_WIDTH: int = RICH_MAX_WIDTH
INDENT_SIZE: int = RICH_INDENT_SIZE
MAX_LENGTH: int | None = RICH_MAX_LENGTH
MAX_STRING: int | None = RICH_MAX_STRING
MAX_DEPTH: int | None = RICH_MAX_DEPTH
EXPAND_ALL: bool = RICH_EXPAND_ALL


def yield_call_args_repr(
    *args: Any,
    _max_width: int = MAX_WIDTH,
    _indent_size: int = INDENT_SIZE,
    _max_length: int | None = MAX_LENGTH,
    _max_string: int | None = MAX_STRING,
    _max_depth: int | None = MAX_DEPTH,
    _expand_all: bool = EXPAND_ALL,
    **kwargs: Any,
) -> Iterator[str]:
    """Pretty print of a set of positional/keyword arguments."""
    mapping = {f"args[{i}]": v for i, v in enumerate(args)} | {
        f"kwargs[{k}]": v for k, v in kwargs.items()
    }
    return yield_mapping_repr(
        _max_width=_max_width,
        _indent_size=_indent_size,
        _max_length=_max_length,
        _max_string=_max_string,
        _max_depth=_max_depth,
        _expand_all=_expand_all,
        **mapping,
    )


def yield_mapping_repr(
    _max_width: int = MAX_WIDTH,
    _indent_size: int = INDENT_SIZE,
    _max_length: int | None = MAX_LENGTH,
    _max_string: int | None = MAX_STRING,
    _max_depth: int | None = MAX_DEPTH,
    _expand_all: bool = EXPAND_ALL,  # noqa: FBT001
    **kwargs: Any,
) -> Iterator[str]:
    """Pretty print of a set of keyword arguments."""
    for k, v in kwargs.items():
        v_repr = pretty_repr(
            v,
            max_width=_max_width,
            indent_size=_indent_size,
            max_length=_max_length,
            max_string=_max_string,
            max_depth=_max_depth,
            expand_all=_expand_all,
        )
        yield f"{k} = {v_repr}"


__all__ = [
    "EXPAND_ALL",
    "INDENT_SIZE",
    "MAX_DEPTH",
    "MAX_LENGTH",
    "MAX_STRING",
    "MAX_WIDTH",
    "yield_call_args_repr",
    "yield_mapping_repr",
]
