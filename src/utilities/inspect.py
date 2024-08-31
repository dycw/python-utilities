from __future__ import annotations

import re
from inspect import signature
from re import MULTILINE, sub
from typing import TYPE_CHECKING, Any

from utilities.reprlib import custom_repr

if TYPE_CHECKING:
    from collections.abc import Callable

_LEFT = "❮"  # noqa: RUF001
_RIGHT = "❯"  # noqa: RUF001
_PATTERN1 = re.compile(f"'{_LEFT}(.*?){_RIGHT}'", flags=MULTILINE)
_PATTERN2 = re.compile(f'"{_LEFT}(.*?){_RIGHT}"', flags=MULTILINE)


def bind_custom_repr(func: Callable[..., Any], *args: Any, **kwargs: Any) -> str:
    """Bind the custom representations of the arguments to a function."""
    args_repr = tuple(map(_bind_custom_repr_bracketed, args))
    kwargs_repr = {k: _bind_custom_repr_bracketed(v) for k, v in kwargs.items()}
    bound_args = signature(func).bind(*args_repr, **kwargs_repr)
    text = sub(r"<BoundArguments \((.*)\)>", r"\1", str(bound_args), flags=MULTILINE)
    while True:
        new = _PATTERN1.sub(r"\1", _PATTERN2.sub(r"\1", text))
        if new == text:
            return text
        text = new


def _bind_custom_repr_bracketed(obj: Any) -> str:
    return "".join([_LEFT, custom_repr(obj), _RIGHT])


__all__ = ["bind_custom_repr"]
