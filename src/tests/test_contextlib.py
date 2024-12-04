from __future__ import annotations

from pytest import raises

from utilities.contextlib import NoOpContextManager


class TestNoOpContextManager:
    def test_main(self) -> None:
        with NoOpContextManager():
            pass

    def test_error(self) -> None:
        with raises(RuntimeError), NoOpContextManager():
            raise RuntimeError
