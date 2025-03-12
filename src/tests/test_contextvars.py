from __future__ import annotations

from utilities.contextvars import _GLOBAL_BREAKPOINT, global_breakpoint


class TestGlobalBreakpoint:
    def test_disabled(self) -> None:
        _ = _GLOBAL_BREAKPOINT.set(False)
        global_breakpoint()
