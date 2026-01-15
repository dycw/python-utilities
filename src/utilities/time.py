from __future__ import annotations

from time import sleep
from typing import TYPE_CHECKING, assert_never

from whenever import DateDelta, DateTimeDelta, Delta, TimeDelta

from utilities.whenever import to_nanoseconds

if TYPE_CHECKING:
    from redis.typing import Number


def sleep_td(delta: Delta | Number | None = None, /) -> None:
    """Sleep which accepts deltas."""
    match delta:
        case DateDelta() | TimeDelta() | DateTimeDelta():
            seconds = to_nanoseconds(delta) / 1e9
        case int() | float() as seconds:
            ...
        case None:
            return
        case never:
            assert_never(never)
    sleep(seconds)


__all__ = ["sleep_td"]
