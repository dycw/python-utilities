from __future__ import annotations

from sys import stderr
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable

    from eventkit import Event


def add_listener(
    event: Event,
    listener: Callable[..., Any],
    /,
    *,
    done: Callable[..., Any] | None = None,
    keep_ref: bool = False,
) -> Event:
    """Connect a listener to an event."""
    return event.connect(
        listener, error=_add_listener_error, done=done, keep_ref=keep_ref
    )


def _add_listener_error(event: Event, exception: Exception, /) -> None:
    """Run callback in the case of an error."""
    try:
        from loguru import logger
    except Exception as error:  # noqa: BLE001  # pragma: no cover
        _ = stderr.write(f"Error running {event}; got {error}")
    else:
        logger.opt(exception=exception).error("Error running {event}", event=event)


__all__ = ["add_listener"]
