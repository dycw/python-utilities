from __future__ import annotations

from re import search
from typing import TYPE_CHECKING

from utilities.constants import SECOND
from utilities.core import sync_sleep
from utilities.threading import run_in_background

if TYPE_CHECKING:
    from threading import Event

    from pytest import CaptureFixture
    from whenever import TimeDelta


_DURATION: TimeDelta = 0.05 * SECOND
_MULTIPLE: int = 10


class TestRunInBackground:
    def test_main(self, *, capsys: CaptureFixture) -> None:
        def counter(event: Event, /) -> None:
            i = 0
            while not event.is_set():
                print(i)  # noqa: T201
                sync_sleep(_DURATION)
                i += 1

        task = run_in_background(counter)
        sync_sleep(_MULTIPLE * _DURATION)
        del task
        stdout = capsys.readouterr().out
        assert search("^0\n1\n", stdout)

    def test_different_signature(self, *, capsys: CaptureFixture) -> None:
        def counter(event: Event, increment: int, /) -> None:
            i = 0
            while not event.is_set():
                print(i)  # noqa: T201
                sync_sleep(_DURATION)
                i += increment

        task = run_in_background(counter, 2)
        sync_sleep(_MULTIPLE * _DURATION)
        del task
        stdout = capsys.readouterr().out
        assert search("^0\n2\n", stdout)
