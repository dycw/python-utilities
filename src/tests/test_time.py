from __future__ import annotations

from typing import TYPE_CHECKING

from utilities.constants import SECOND
from utilities.time import sleep
from utilities.timer import Timer

if TYPE_CHECKING:
    from whenever import TimeDelta


_DURATION: TimeDelta = 0.05 * SECOND
_MULTIPLE: int = 1


class TestSleep:
    def test_main(self) -> None:
        with Timer() as timer:
            sleep(_DURATION)
        assert timer <= _MULTIPLE * _DURATION

    def test_none(self) -> None:
        sleep()
