from __future__ import annotations

from beartype.roar import BeartypeCallHintParamViolation
from pytest import raises

from tests.beartype.functions import Example, identity


class TestBeartypeIfDev:
    def test_main(self) -> None:
        assert identity(0) == 0
        with raises(BeartypeCallHintParamViolation):
            _ = identity(0.0)  # type: ignore

    def test_dataclass(self) -> None:
        assert Example(0).x == 0
        with raises(BeartypeCallHintParamViolation):
            _ = Example(0.0)  # type: ignore
