from __future__ import annotations

from typing import TYPE_CHECKING, Any

from rich.pretty import pretty_repr

if TYPE_CHECKING:
    from collections.abc import Iterator


def yield_pretty_repr_args_and_kwargs(*args: Any, **kwargs: Any) -> Iterator[str]:
    """Pretty print of positional/keyword arguments."""
    for i, arg in enumerate(args):
        yield f"args[{i}] = {pretty_repr(arg)}"
    for k, v in kwargs.items():
        yield f"kwargs[{k}] = {pretty_repr(v)}"


__all__ = ["yield_pretty_repr_args_and_kwargs"]
