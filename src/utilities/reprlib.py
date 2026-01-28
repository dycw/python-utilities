from __future__ import annotations

from typing import TYPE_CHECKING

from rich.pretty import pretty_repr

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
    max_width: int = RICH_MAX_WIDTH,
    indent_size: int = RICH_INDENT_SIZE,
    max_length: int | None = RICH_MAX_LENGTH,
    max_string: int | None = RICH_MAX_STRING,
    max_depth: int | None = RICH_MAX_DEPTH,
    expand_all: bool = RICH_EXPAND_ALL,
) -> Iterator[str]:
    """Pretty print of a set of keyword arguments."""
    for k, v in mapping.items():
        repr_use = pretty_repr(
            v,
            max_width=max_width,
            indent_size=indent_size,
            max_length=max_length,
            max_string=max_string,
            max_depth=max_depth,
            expand_all=expand_all,
        )
        yield f"{k} = {repr_use}"


__all__ = ["yield_mapping_repr"]
