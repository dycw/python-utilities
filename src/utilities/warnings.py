from collections.abc import Iterator
from contextlib import ExitStack
from contextlib import contextmanager
from typing import Literal
from warnings import catch_warnings
from warnings import filterwarnings

from beartype import beartype


@beartype
def catch_warnings_as_errors(
    *,
    message: str = "",
    category: type[Warning] | tuple[type[Warning], ...] | None = None,
) -> ExitStack:
    """Catch warnings as errors."""

    return _handle_warnings("error", message=message, category=category)


@beartype
def suppress_warnings(
    *,
    message: str = "",
    category: type[Warning] | tuple[type[Warning], ...] | None = None,
) -> ExitStack:
    """Suppress warnings."""

    return _handle_warnings("ignore", message=message, category=category)


_ActionKind = Literal["error", "ignore"]


@beartype
def _handle_warnings(
    action: _ActionKind,
    /,
    *,
    message: str = "",
    category: type[Warning] | tuple[type[Warning], ...] | None = None,
) -> ExitStack:
    """Suppress warnings."""

    stack = ExitStack()
    if isinstance(category, tuple):
        categories = category
    else:
        categories = [category]
    for cat in categories:
        cm = _handle_warnings_1(action, message=message, category=cat)
        stack.enter_context(cm)
    return stack


@contextmanager
@beartype
def _handle_warnings_1(
    action: _ActionKind,
    /,
    *,
    message: str = "",
    category: type[Warning] | None = None,
) -> Iterator[None]:
    with catch_warnings():
        kwargs = {} if category is None else {"category": category}
        filterwarnings(action, message=message, **kwargs)
        yield
