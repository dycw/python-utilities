from __future__ import annotations

from contextvars import ContextVar

from utilities.contextvars import (
    _GLOBAL_BREAKPOINT,
    global_breakpoint,
    set_global_breakpoint,
    yield_set_context,
)
from utilities.text import unique_str


class TestGlobalBreakpoint:
    def test_disabled(self) -> None:
        global_breakpoint()

    def test_set(self) -> None:
        try:
            set_global_breakpoint()
        finally:
            _ = _GLOBAL_BREAKPOINT.set(False)


class TestYieldSetContext:
    def test_disabled(self) -> None:
        context = ContextVar(unique_str(), default=False)
        assert not context.get()
        with yield_set_context(context):
            assert context.get()
        assert not context.get()

    def test_set(self) -> None:
        try:
            set_global_breakpoint()
        finally:
            _ = _GLOBAL_BREAKPOINT.set(False)
