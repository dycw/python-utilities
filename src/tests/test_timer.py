from __future__ import annotations

from operator import add, eq, ge, gt, le, lt, mul, ne, sub, truediv
from re import search
from typing import TYPE_CHECKING, Any

from pytest import mark, param, raises
from whenever import TimeDelta

from utilities.asyncio import sleep_td
from utilities.timer import Timer
from utilities.whenever import SECOND, ZERO_TIME

if TYPE_CHECKING:
    from collections.abc import Callable


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
    async def test_arithmetic_against_another_timer(
        self, *, op: Callable[[Any, Any], Any], cls: type[Any]
    ) -> None:
        with Timer() as timer1, Timer() as timer2:
            await sleep_td(0.01 * SECOND)
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

    async def test_context_manager(self) -> None:
        delta = 0.1 * SECOND
        with Timer() as timer:
            await sleep_td(2 * delta)
        assert timer >= delta

    def test_hashable(self) -> None:
        timer = Timer()
        _ = hash(timer)

    @mark.parametrize("func", [param(repr), param(str)])
    async def test_repr_and_str(self, *, func: Callable[[Timer], str]) -> None:
        with Timer() as timer:
            await sleep_td(0.01 * SECOND)
        as_str = func(timer)
        assert search(r"^PT0\.\d+S$", as_str)

    async def test_running(self) -> None:
        delta = 0.1 * SECOND
        timer = Timer()
        await sleep_td(2 * delta)
        assert timer >= delta
        await sleep_td(2 * delta)
        assert timer >= 2 * delta

    def test_timedelta(self) -> None:
        timer = Timer()
        assert isinstance(timer.timedelta, TimeDelta)
