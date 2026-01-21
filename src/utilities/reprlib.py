from __future__ import annotations

from functools import partial
from typing import TYPE_CHECKING

from utilities.constants import (
    RICH_EXPAND_ALL,
    RICH_INDENT_SIZE,
    RICH_MAX_DEPTH,
    RICH_MAX_LENGTH,
    RICH_MAX_STRING,
    RICH_MAX_WIDTH,
)

if TYPE_CHECKING:
    from collections.abc import Iterator

    from utilities.types import StrMapping


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


__all__ = ["yield_mapping_repr"]
