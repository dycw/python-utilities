from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar

from utilities.time import sleep_td
from utilities.timer import Timer
from utilities.whenever import SECOND

if TYPE_CHECKING:
    from whenever import TimeDelta


class TestSleepTD:
    multiple: ClassVar[int] = 5
    delta: ClassVar[TimeDelta] = 0.05 * SECOND

    def test_main(self) -> None:
        with Timer() as timer:
            sleep_td(self.delta)
        assert timer <= self.multiple * self.delta

    def test_number(self) -> None:
        with Timer() as timer:
            sleep_td(self.delta.in_seconds())
        assert timer <= self.multiple * self.delta

    def test_none(self) -> None:
        with Timer() as timer:
            sleep_td()
        assert timer <= self.multiple * self.delta
