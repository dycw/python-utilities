from __future__ import annotations

from inspect import signature
from re import MULTILINE, sub
from typing import TYPE_CHECKING, Any

from utilities.reprlib import custom_repr

if TYPE_CHECKING:
    from collections.abc import Callable
    from inspect import BoundArguments


def bind_custom_repr(
    func: Callable[..., Any], *args: Any, **kwargs: Any
) -> BoundArguments:
    """Bind the custom representations of the arguments to a function."""
    args_repr = tuple(map(custom_repr, args))
    kwargs_repr = {k: custom_repr(v) for k, v in kwargs.items()}
    return signature(func).bind(*args_repr, **kwargs_repr)


def extract_bound_args_repr(bound_args: BoundArguments, /) -> str:
    return sub(r"<BoundArguments \((.*)\)>", r"\1", str(bound_args), flags=MULTILINE)


__all__ = ["bind_custom_repr", "extract_bound_args_repr"]
