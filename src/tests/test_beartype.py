from __future__ import annotations

from dataclasses import dataclass
from typing import Any, cast

from beartype.roar import BeartypeCallHintParamViolation
from pytest import raises

from utilities.beartype import beartype_if_dev


@beartype_if_dev
def identity(x: int, /) -> int:
    return x


@beartype_if_dev
@dataclass
class Example:
    x: int


class TestBeartypeIfDev:
    def test_main(self) -> None:
        assert identity(0) == 0
        with raises(BeartypeCallHintParamViolation):
            _ = identity(cast(Any, 0.0))

    def test_dataclass(self) -> None:
        assert Example(0).x == 0
        with raises(BeartypeCallHintParamViolation):
            _ = Example(cast(Any, 0.0))


if __name__ == "__main__":
    # these should pass:
    _ = identity(cast(Any, 0.0))
    _ = Example(cast(Any, 0.0))
