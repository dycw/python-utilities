from __future__ import annotations

from typing import TYPE_CHECKING

from utilities.constants import SECOND
from utilities.core import sync_sleep
from utilities.timer import Timer

if TYPE_CHECKING:
    from whenever import TimeDelta


_DURATION: TimeDelta = 0.05 * SECOND
_MULTIPLE: int = 10


class TestSyncSleep:
    def test_main(self) -> None:
        with Timer() as timer:
            sync_sleep(_DURATION)
        assert timer <= _MULTIPLE * _DURATION

    def test_none(self) -> None:
        sync_sleep()
