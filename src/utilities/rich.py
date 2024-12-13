from __future__ import annotations

from typing import TYPE_CHECKING, Any

from rich.pretty import pretty_repr

if TYPE_CHECKING:
    from collections.abc import Iterator


def yield_call_args_repr(*args: Any, **kwargs: Any) -> Iterator[str]:
    """Pretty print of a set of positional/keyword arguments."""
    mapping = {f"args[{i}]": v for i, v in enumerate(args)} | {
        f"kwargs[{k}]": v for k, v in kwargs.items()
    }
    return yield_mapping_repr(**mapping)


def yield_mapping_repr(**kwargs: Any) -> Iterator[str]:
    """Pretty print of a set of keyword arguments."""
    for k, v in kwargs.items():
        yield f"{k} = {pretty_repr(v)}"


__all__ = ["yield_call_args_repr", "yield_mapping_repr"]
