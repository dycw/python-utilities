from __future__ import annotations

import time
from typing import TYPE_CHECKING, assert_never

from whenever import TimeDelta

if TYPE_CHECKING:
    from utilities.types import Duration


def sleep(duration: Duration | None = None, /) -> None:
    """Sleep which accepts deltas."""
    match duration:
        case int() | float() as seconds:
            ...
        case TimeDelta():
            seconds = duration.in_seconds()
        case None:
            return
        case never:
            assert_never(never)
    time.sleep(seconds)


__all__ = ["sleep"]
