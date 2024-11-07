from __future__ import annotations

import sys
from functools import partial
from typing import TYPE_CHECKING, Any

from utilities.functions import get_class_name

if TYPE_CHECKING:
    from collections.abc import Callable

    from eventkit import Event


def add_listener(
    event: Event,
    listener: Callable[..., Any],
    /,
    *,
    error: Callable[[Event, Exception], None] | None = None,
    done: Callable[..., Any] | None = None,
    keep_ref: bool = False,
    _stdout: bool = True,
    _loguru: bool = True,
) -> Event:
    """Connect a listener to an event."""
    error_default = partial(_add_listener_error, stdout=_stdout, loguru=_loguru)
    if error is None:
        error_use = error_default
    else:

        def combined(event: Event, exception: Exception, /) -> None:
            _add_listener_error(event, exception)
            error_default(event, exception)

        error_use = combined
    return event.connect(listener, error=error_use, done=done, keep_ref=keep_ref)


def _add_listener_error(
    event: Event, exception: Exception, /, *, stdout: bool = True, loguru: bool = True
) -> None:
    """Run callback in the case of an error."""
    type_name = get_class_name(exception)
    event_name = event.name()
    desc = f"{type_name} running {event_name}"
    if stdout:
        msg = f"{desc}:\t\n{event=}\n{exception=}"
        _ = sys.stdout.write(f"{msg}\n")
    if loguru:
        try:
            from loguru import logger
        except ModuleNotFoundError:  # pragma: no cover
            pass
        else:
            logger.opt(exception=exception).error(f"{{desc}}:\t\n{event}", event=event)


__all__ = ["add_listener"]
