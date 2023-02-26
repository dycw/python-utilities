import datetime as dt
from collections.abc import Callable
from operator import eq
from operator import ge
from operator import gt
from operator import le
from operator import lt
from operator import ne
from re import search
from time import sleep
from typing import Any

from pytest import mark
from pytest import param
from pytest import raises

from utilities.timer import Timer


class TestTimer:
    def test_context_manager(self) -> None:
        with Timer() as timer:
            assert isinstance(timer, Timer)
            sleep(1e-3)
        assert timer >= 1e-3

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
    @mark.parametrize(
        "dur",
        [param(1), param(1.0), param(dt.timedelta(seconds=1))],
    )
    def test_comparison(
        self,
        op: Callable[[Any, Any], bool],
        dur: Any,
        expected: bool,
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
        with Timer() as timer:
            pass
        with raises(TypeError):
            _ = timer == "error"

    @mark.parametrize("func", [param(repr), param(str)])
    def test_repr_and_str(self, func: Callable[[Timer], str]) -> None:
        with Timer() as timer:
            pass
        as_str = func(timer)
        assert search(r"^\d+:\d{2}:\d{2}\.\d{6}$", as_str)

    def test_running(self) -> None:
        timer = Timer()
        sleep(1e-3)
        assert timer >= 1e-3
        sleep(1e-3)
        assert timer >= 2e-3

    def test_timedelta(self) -> None:
        timer = Timer()
        assert isinstance(timer.timedelta, dt.timedelta)
