from __future__ import annotations

from contextlib import ExitStack
from typing import TYPE_CHECKING, Literal, TypedDict
from warnings import catch_warnings, filterwarnings

from utilities.contextlib import enhanced_context_manager

if TYPE_CHECKING:
    from collections.abc import Iterator

    from utilities.types import TypeLike


@enhanced_context_manager
def catch_warnings_as_errors(
    *, message: str = "", category: TypeLike[Warning] | None = None
) -> Iterator[None]:
    """Catch warnings as errors."""
    with _handle_warnings("error", message=message, category=category):
        yield


@enhanced_context_manager
def suppress_warnings(
    *, message: str = "", category: TypeLike[Warning] | None = None
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
    category: TypeLike[Warning] | None = None,
) -> ExitStack:
    """Handle a set of warnings."""
    stack = ExitStack()
    categories = category if isinstance(category, tuple) else [category]
    for cat in categories:
        cm = _handle_warnings_1(action, message=message, category=cat)
        stack.enter_context(cm)
    return stack


@enhanced_context_manager
def _handle_warnings_1(
    action: _ActionKind, /, *, message: str = "", category: type[Warning] | None = None
) -> Iterator[None]:
    """Handle one set of warnings."""

    class Kwargs(TypedDict, total=False):
        category: type[Warning]

    with catch_warnings():
        kwargs: Kwargs = {} if category is None else {"category": category}
        filterwarnings(action, message=message, **kwargs)
        yield


__all__ = ["catch_warnings_as_errors", "suppress_warnings"]
