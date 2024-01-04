from __future__ import annotations

from beartype.roar import BeartypeCallHintParamViolation
from pytest import raises

from utilities.beartype import beartype_if_pytest


class TestBeartypeIfPytest:
    def test_main(self) -> None:
        @beartype_if_pytest
        def func(x: int, /) -> int:
            return x

        assert func(0) == 0
        with raises(BeartypeCallHintParamViolation):
            _ = func(0.0)  # type: ignore
