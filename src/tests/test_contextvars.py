from __future__ import annotations

from utilities.contextvars import (
    _GLOBAL_BREAKPOINT,
    global_breakpoint,
    set_global_breakpoint,
)


class TestGlobalBreakpoint:
    def test_disabled(self) -> None:
        global_breakpoint()

    def test_set(self) -> None:
        try:
            set_global_breakpoint()
        finally:
            _ = _GLOBAL_BREAKPOINT.set(False)
