from __future__ import annotations

from dataclasses import dataclass
from itertools import chain
from threading import Event, Thread
from typing import TYPE_CHECKING, Any, Concatenate, ParamSpec

if TYPE_CHECKING:
    from collections.abc import Callable


_P = ParamSpec("_P")


@dataclass(frozen=True, kw_only=True)
class BackgroundTask:
    """An event and daemon thread, running a task in the background."""

    event: Event
    thread: Thread

    def __post_init__(self) -> None:
        self.thread.start()

    def __del__(self) -> None:
        self.event.set()
        self.thread.join()


def run_in_background(
    func: Callable[Concatenate[Event, _P], Any], *args: _P.args, **kwargs: _P.kwargs
) -> BackgroundTask:
    """Run a function in the background."""
    event = Event()
    thread = Thread(
        target=func, args=tuple(chain([event], args)), kwargs=kwargs, daemon=True
    )
    return BackgroundTask(event=event, thread=thread)


__all__ = ["BackgroundTask", "run_in_background"]
