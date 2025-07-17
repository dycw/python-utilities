from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterator


##


_GLOBAL_BREAKPOINT = ContextVar("GLOBAL_BREAKPOINT", default=False)


def global_breakpoint() -> None:
    """Set a breakpoint if the global breakpoint is enabled."""
    if _GLOBAL_BREAKPOINT.get():  # pragma: no cover
        breakpoint()  # noqa: T100


def set_global_breakpoint() -> None:
    """Set the global breakpoint ."""
    _ = _GLOBAL_BREAKPOINT.set(True)


##


@contextmanager
def yield_set_context(var: ContextVar[bool], /) -> Iterator[None]:
    """Yield a context var as being set."""
    token = var.set(True)
    try:
        yield
    finally:
        _ = var.reset(token)


__all__ = ["global_breakpoint", "set_global_breakpoint", "yield_set_context"]
