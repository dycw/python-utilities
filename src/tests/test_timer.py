from __future__ import annotations

import datetime as dt
from operator import add, eq, ge, gt, le, lt, mul, ne, sub, truediv
from re import search
from time import sleep
from typing import TYPE_CHECKING, Any

from pytest import mark, param, raises

from utilities.datetime import SECOND, ZERO_TIME
from utilities.timer import Timer

if TYPE_CHECKING:
    from collections.abc import Callable


class TestTimer:
    @mark.parametrize(
        ("op", "other", "cls"),
        [
            param(add, 0, dt.timedelta),
            param(add, 0.0, dt.timedelta),
            param(add, ZERO_TIME, dt.timedelta),
            param(sub, 0, dt.timedelta),
            param(sub, 0.0, dt.timedelta),
            param(sub, ZERO_TIME, dt.timedelta),
            param(mul, 1, dt.timedelta),
            param(mul, 1.0, dt.timedelta),
            param(truediv, 1, dt.timedelta),
            param(truediv, 1.0, dt.timedelta),
            param(truediv, SECOND, float),
        ],
        ids=str,
    )
    def test_arithmetic_against_numbers_or_timedeltas(
        self, *, op: Callable[[Any, Any], Any], other: Any, cls: type[Any]
    ) -> None:
        with Timer() as timer:
            pass
        assert isinstance(op(timer, other), cls)

    @mark.parametrize(
        ("op", "cls"),
        [param(add, dt.timedelta), param(sub, dt.timedelta), param(truediv, float)],
        ids=str,
    )
    def test_arithmetic_against_another_timer(
        self, *, op: Callable[[Any, Any], Any], cls: type[Any]
    ) -> None:
        with Timer() as timer1, Timer() as timer2:
            sleep(0.01)
        assert isinstance(op(timer1, timer2), cls)

    @mark.parametrize(
        ("op", "other"),
        [
            param(add, ""),
            param(sub, ""),
            param(mul, SECOND),
            param(mul, ""),
            param(truediv, ""),
        ],
        ids=str,
    )
    def test_arithmetic_error(
        self, *, op: Callable[[Any, Any], Any], other: Any
    ) -> None:
        with Timer() as timer:
            pass
        with raises(TypeError):
            _ = op(timer, other)

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
        ids=str,
    )
    @mark.parametrize(
        "dur", [param(1), param(1.0), param(dt.timedelta(seconds=1))], ids=str
    )
    def test_comparison(
        self, *, op: Callable[[Any, Any], bool], dur: Any, expected: bool
    ) -> None:
        with Timer() as timer:
            pass
        assert op(timer, dur) is expected

    @mark.parametrize(
        "op",
        [param(eq), param(ne), param(ge), param(gt), param(le), param(lt)],
        ids=str,
    )
    def test_comparison_between_timers(self, *, op: Callable[[Any, Any], bool]) -> None:
        with Timer() as timer1:
            pass
        with Timer() as timer2:
            pass
        assert isinstance(op(timer1, timer2), bool)

    @mark.parametrize(("op", "expected"), [param(eq, False), param(ne, True)], ids=str)
    def test_comparison_eq_and_ne(
        self, *, op: Callable[[Any, Any], bool], expected: bool
    ) -> None:
        with Timer() as timer:
            pass
        assert op(timer, "") is expected

    @mark.parametrize("op", [param(ge), param(gt), param(le), param(lt)], ids=str)
    def test_comparison_error(self, *, op: Callable[[Any, Any], bool]) -> None:
        with Timer() as timer:
            pass
        with raises(TypeError):
            _ = op(timer, "")

    def test_context_manager(self) -> None:
        duration = 0.01
        with Timer() as timer:
            assert isinstance(timer, Timer)
            sleep(2 * duration)
        assert timer >= duration

    @mark.parametrize("func", [param(repr), param(str)], ids=str)
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
