import datetime as dt
from collections.abc import Callable
from operator import eq
from operator import ge
from operator import gt
from operator import le
from operator import lt
from operator import ne
from time import sleep
from typing import Any

from _pytest.python_api import raises
from pytest import mark
from pytest import param

from dycw_utilities.timer import Timer


class TestTimer:
    def test_context_manager(self) -> None:
        with Timer() as timer:
            assert isinstance(timer, Timer)
            sleep(1e-3)
        assert timer >= 1e-3

    @mark.parametrize(
        "op, expected",
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
        "dur", [param(1), param(1.0), param(dt.timedelta(seconds=1))]
    )
    def test_comparison(
        self, op: Callable[[Any, Any], bool], dur: Any, expected: bool
    ) -> None:
        with Timer() as timer:
            sleep(1e-3)
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
