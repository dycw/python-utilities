from __future__ import annotations

import datetime as dt
from operator import eq, ge, gt, le, lt, ne
from re import search
from time import sleep
from typing import TYPE_CHECKING, Any

from pytest import mark, param, raises

from utilities.timer import Timer, TimerError

if TYPE_CHECKING:
    from collections.abc import Callable


class TestTimer:
    def test_context_manager(self) -> None:
        duration = 0.01
        with Timer() as timer:
            assert isinstance(timer, Timer)
            sleep(2 * duration)
        assert timer >= duration

    @mark.parametrize(
        ("op", "expected"),
        [
            param(eq, False),
            param(ne, True),
            param(ge, False),
            param(gt, False),
            param(le, True),
            param(lt, True),
        ],
    )
    @mark.parametrize("dur", [param(1), param(1.0), param(dt.timedelta(seconds=1))])
    def test_comparison(
        self, *, op: Callable[[Any, Any], bool], dur: Any, expected: bool
    ) -> None:
        with Timer() as timer:
            pass
        assert op(timer, dur) is expected

    def test_comparison_between_timers(self) -> None:
        with Timer() as timer1:
            pass
        with Timer() as timer2:
            pass
        assert isinstance(timer1 == timer2, bool)

    def test_comparison_error(self) -> None:
        match = (
            "Timer must be compared to a number, Timer, or timedelta; got .* instead"
        )
        with raises(TimerError, match=match):
            _ = Timer() == "error"

    @mark.parametrize("func", [param(repr), param(str)])
    def test_repr_and_str(self, *, func: Callable[[Timer], str]) -> None:
        with Timer() as timer:
            sleep(0.01)
        as_str = func(timer)
        assert search(r"^\d+:\d{2}:\d{2}\.\d{6}$", as_str)

    def test_running(self) -> None:
        duration = 0.01
        timer = Timer()
        sleep(2 * duration)
        assert timer >= duration
        sleep(2 * duration)
        assert timer >= 2 * duration

    def test_timedelta(self) -> None:
        timer = Timer()
        assert isinstance(timer.timedelta, dt.timedelta)
