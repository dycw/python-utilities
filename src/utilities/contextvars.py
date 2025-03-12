from __future__ import annotations

from contextvars import ContextVar

##


_GLOBAL_BREAKPOINT = ContextVar("GLOBAL_BREAKPOINT", default=False)


def global_breakpoint() -> None:
    """Set a breakpoint if the global breakpoint is enabled."""
    if _GLOBAL_BREAKPOINT.get():  # pragma: no cover
        breakpoint()  # noqa: T100


def set_global_breakpoint() -> None:
    """Set the global breakpoint ."""
    _ = _GLOBAL_BREAKPOINT.set(True)


__all__ = ["global_breakpoint", "set_global_breakpoint"]
