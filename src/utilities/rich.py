from __future__ import annotations

from typing import TYPE_CHECKING, Any

from rich.pretty import pretty_repr

if TYPE_CHECKING:
    from collections.abc import Iterator

MAX_WIDTH: int = 80
INDENT_SIZE: int = 4
MAX_LENGTH: int | None = None
MAX_STRING: int | None = None
MAX_DEPTH: int | None = None
EXPAND_ALL: bool = False


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
