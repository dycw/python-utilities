from __future__ import annotations

from random import Random, SystemRandom
from typing import TYPE_CHECKING, TypeVar

if TYPE_CHECKING:
    from collections.abc import Iterable


_T = TypeVar("_T")

SYSTEM_RANDOM = SystemRandom()


def get_state(
    *, seed: float | str | bytes | bytearray | Random | None = None
) -> Random:
    """Get a random state."""
    if isinstance(seed, Random):
        return seed
    return Random(x=seed)


def shuffle(
    iterable: Iterable[_T],
    /,
    *,
    seed: float | str | bytes | bytearray | Random | None = None,
) -> list[_T]:
    """Shuffle an iterable."""
    copy = list(iterable).copy()
    state = get_state(seed=seed)
    state.shuffle(copy)
    return copy


__all__ = ["SYSTEM_RANDOM", "get_state", "shuffle"]
