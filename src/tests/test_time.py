from __future__ import annotations

from utilities.time import sleep
from utilities.timer import Timer


class TestSleep:
    def test_main(self) -> None:
        delta = 0.1
        with Timer() as timer:
            sleep(delta)
        assert timer <= 10 * delta

    def test_none(self) -> None:
        sleep()
