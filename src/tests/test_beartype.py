from __future__ import annotations

from dataclasses import dataclass

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
            _ = identity(0.0)  # type: ignore[]

    def test_dataclass(self) -> None:
        assert Example(0).x == 0
        with raises(BeartypeCallHintParamViolation):
            _ = Example(0.0)  # type: ignore[]


if __name__ == "__main__":
    # these should pass:
    _ = identity(0.0)  # type: ignore[]
    _ = Example(0.0)  # type: ignore[]
