from __future__ import annotations

from operator import add, eq, ge, gt, le, lt, mul, ne, sub, truediv
from re import search
from typing import TYPE_CHECKING, Any

from pytest import mark, param, raises
from whenever import TimeDelta

from utilities.constants import SECOND, ZERO_TIME
from utilities.time import sleep
from utilities.timer import Timer

if TYPE_CHECKING:
    from collections.abc import Callable


_DURATION: TimeDelta = 0.05 * SECOND
_MULTIPLE: int = 2


class TestTimer:
    @mark.parametrize(
        ("op", "other"),
        [
            param(add, ZERO_TIME),
            param(sub, ZERO_TIME),
            param(mul, 1),
            param(mul, 1.0),
            param(truediv, 1),
            param(truediv, 1.0),
        ],
    )
    def test_arithmetic_against_numbers_or_timedeltas(
        self, *, op: Callable[[Any, Any], Any], other: Any
    ) -> None:
        with Timer() as timer:
            pass
        assert isinstance(op(timer, other), TimeDelta)

    @mark.parametrize(
        ("op", "cls"),
        [param(add, TimeDelta), param(sub, TimeDelta), param(truediv, float)],
    )
    def test_arithmetic_against_another_timer(
        self, *, op: Callable[[Any, Any], Any], cls: type[Any]
    ) -> None:
        with Timer() as timer1, Timer() as timer2:
            sleep(_DURATION)
        assert isinstance(op(timer1, timer2), cls)

    @mark.parametrize(("op"), [param(add), param(sub), param(mul), param(truediv)])
    def test_arithmetic_error(self, *, op: Callable[[Any, Any], Any]) -> None:
        with Timer() as timer:
            pass
        with raises(TypeError):
            _ = op(timer, "")

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
    def test_comparison(
        self, *, op: Callable[[Any, Any], bool], expected: bool
    ) -> None:
        with Timer() as timer:
            pass
        assert op(timer, SECOND) is expected

    @mark.parametrize(
        "op", [param(eq), param(ne), param(ge), param(gt), param(le), param(lt)]
    )
    def test_comparison_between_timers(self, *, op: Callable[[Any, Any], bool]) -> None:
        with Timer() as timer1:
            pass
        with Timer() as timer2:
            pass
        assert isinstance(op(timer1, timer2), bool)

    @mark.parametrize(("op", "expected"), [param(eq, False), param(ne, True)])
    def test_comparison_eq_and_ne(
        self, *, op: Callable[[Any, Any], bool], expected: bool
    ) -> None:
        with Timer() as timer:
            pass
        assert op(timer, "") is expected

    @mark.parametrize("op", [param(ge), param(gt), param(le), param(lt)])
    def test_comparison_error(self, *, op: Callable[[Any, Any], bool]) -> None:
        with Timer() as timer:
            pass
        with raises(TypeError):
            _ = op(timer, "")

    def test_context_manager(self) -> None:
        duration = 0.1
        with Timer() as timer:
            sleep(2 * duration)
        assert timer >= duration

    def test_float(self) -> None:
        with Timer() as timer:
            ...
        assert float(timer) == timer.timedelta.in_seconds()

    def test_hashable(self) -> None:
        timer = Timer()
        _ = hash(timer)

    @mark.parametrize("func", [param(repr), param(str)])
    def test_repr_and_str(self, *, func: Callable[[Timer], str]) -> None:
        with Timer() as timer:
            sleep(_DURATION)
        as_str = func(timer)
        assert search(r"^PT0\.\d+S$", as_str)

    def test_running(self) -> None:
        timer = Timer()
        sleep(_MULTIPLE * _DURATION)
        assert timer >= _DURATION
        sleep(_MULTIPLE * _DURATION)
        assert timer >= 2 * _MULTIPLE * _DURATION

    def test_timedelta(self) -> None:
        timer = Timer()
        assert isinstance(timer.timedelta, TimeDelta)
