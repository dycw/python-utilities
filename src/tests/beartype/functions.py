from __future__ import annotations

from dataclasses import dataclass

from utilities.beartype import beartype_if_dev


@beartype_if_dev
def identity(x: int, /) -> int:
    return x


@beartype_if_dev
@dataclass
class Example:
    x: int


if __name__ == "__main__":
    # these should pass:
    _ = identity(0.0)  # type: ignore
    _ = Example(0.0)  # type: ignore
