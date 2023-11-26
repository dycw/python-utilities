from __future__ import annotations

from collections.abc import Iterator
from contextlib import ExitStack, contextmanager
from typing import Literal, TypedDict, cast
from warnings import catch_warnings, filterwarnings


@contextmanager
def catch_warnings_as_errors(
    *,
    message: str = "",
    category: type[Warning] | tuple[type[Warning], ...] | None = None,
) -> Iterator[None]:
    """Catch warnings as errors."""
    with _handle_warnings("error", message=message, category=category):
        yield


@contextmanager
def suppress_warnings(
    *,
    message: str = "",
    category: type[Warning] | tuple[type[Warning], ...] | None = None,
) -> Iterator[None]:
    """Suppress warnings."""
    with _handle_warnings("ignore", message=message, category=category):
        yield


_ActionKind = Literal["error", "ignore"]


def _handle_warnings(
    action: _ActionKind,
    /,
    *,
    message: str = "",
    category: type[Warning] | tuple[type[Warning], ...] | None = None,
) -> ExitStack:
    """Handle a set of warnings."""
    stack = ExitStack()
    categories = category if isinstance(category, tuple) else [category]
    for cat in categories:
        cm = _handle_warnings_1(action, message=message, category=cat)
        stack.enter_context(cm)
    return stack


@contextmanager
def _handle_warnings_1(
    action: _ActionKind, /, *, message: str = "", category: type[Warning] | None = None
) -> Iterator[None]:
    """Handle one set of warnings."""

    class Kwargs(TypedDict, total=False):
        category: type[Warning]

    with catch_warnings():
        kwargs = cast(Kwargs, {} if category is None else {"category": category})
        filterwarnings(action, message=message, **kwargs)
        yield


__all__ = ["catch_warnings_as_errors", "suppress_warnings"]
