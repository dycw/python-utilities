from __future__ import annotations

from bdb import BdbQuit

from pytest import raises

from utilities.contextvars import (
    _GLOBAL_BREAKPOINT,
    global_breakpoint,
    set_global_breakpoint,
)


class TestGlobalBreakpoint:
    def test_disabled(self) -> None:
        _ = _GLOBAL_BREAKPOINT.set(False)
        global_breakpoint()

    def test_enabled(self) -> None:
        set_global_breakpoint()
        with raises(BdbQuit, match=""):
            global_breakpoint()
