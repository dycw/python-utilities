from __future__ import annotations

from typing import TYPE_CHECKING, TypeVar

from eventkit import Event
from hypothesis import HealthCheck, given, settings
from hypothesis.strategies import integers

from utilities.eventkit import add_listener
from utilities.functions import identity

if TYPE_CHECKING:
    from pytest import CaptureFixture

_T = TypeVar("_T")


class TestAddListener:
    @given(n=integers())
    @settings(suppress_health_check={HealthCheck.function_scoped_fixture})
    async def test_main(self, *, capsys: CaptureFixture, n: int) -> None:
        def func(obj: _T, /) -> _T:
            print(obj)  # noqa: T201
            return identity(obj)

        event = Event()
        _ = add_listener(event, func)
        event.emit(n)
        out = capsys.readouterr().out
        assert out == f"{n}\n"
