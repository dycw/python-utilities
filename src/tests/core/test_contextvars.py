from __future__ import annotations

from contextvars import ContextVar

from utilities.core import unique_str, yield_temp_context_var


class TestYieldTempContextVar:
    def test_disabled(self) -> None:
        context = ContextVar(unique_str(), default=False)
        assert not context.get()
        with yield_temp_context_var(context):
            assert context.get()
        assert not context.get()
