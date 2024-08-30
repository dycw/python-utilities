from __future__ import annotations

from re import search
from time import sleep
from typing import TYPE_CHECKING

from utilities.threading import run_in_background

if TYPE_CHECKING:
    from threading import Event

    from pytest import CaptureFixture


class TestRunInBackground:
    def test_main(self, *, capsys: CaptureFixture) -> None:
        def counter(event: Event, /) -> None:
            i = 0
            while not event.is_set():
                print(i)  # noqa: T201
                sleep(0.01)
                i += 1

        task = run_in_background(counter)
        sleep(0.1)
        del task
        stdout = capsys.readouterr().out
        assert search("^0\n1\n", stdout)

    def test_different_signature(self, *, capsys: CaptureFixture) -> None:
        def counter(event: Event, increment: int, /) -> None:
            i = 0
            while not event.is_set():
                print(i)  # noqa: T201
                sleep(0.01)
                i += increment

        task = run_in_background(counter, 2)
        sleep(0.1)
        del task
        stdout = capsys.readouterr().out
        assert search("^0\n2\n", stdout)
