from __future__ import annotations

from dataclasses import dataclass
from itertools import chain
from threading import Event, Thread
from typing import TYPE_CHECKING, Any, ParamSpec

if TYPE_CHECKING:
    from collections.abc import Callable


_P = ParamSpec("_P")


@dataclass(frozen=True, kw_only=True)
class _BackgroundTask:
    event: Event
    thread: Thread

    def __post_init__(self) -> None:
        self.thread.start()

    def __del__(self) -> None:
        self.event.set()
        self.thread.join()


def run_in_background(
    func: Callable[_P, Any], *args: _P.args, **kwargs: _P.kwargs
) -> _BackgroundTask:
    """Run a function in the background."""
    event = Event()
    thread = Thread(
        target=func, args=tuple(chain([event], args)), kwargs=kwargs, daemon=True
    )
    return _BackgroundTask(event=event, thread=thread)


__all__ = ["run_in_background"]
