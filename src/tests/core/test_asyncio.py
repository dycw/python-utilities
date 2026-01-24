from __future__ import annotations

from typing import TYPE_CHECKING

from utilities.constants import SECOND
from utilities.core import async_sleep
from utilities.timer import Timer

if TYPE_CHECKING:
    from whenever import TimeDelta


_DURATION: TimeDelta = 0.05 * SECOND
_MULTIPLE = 10


class TestAsyncSleep:
    async def test_main(self) -> None:
        with Timer() as timer:
            await async_sleep(_DURATION)
        assert timer <= _MULTIPLE * _DURATION

    async def test_none(self) -> None:
        await async_sleep()
