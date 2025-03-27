from __future__ import annotations

from asyncio import iscoroutinefunction
from functools import wraps
from typing import TYPE_CHECKING, Any, cast

from utilities.functions import apply_decorators
from utilities.iterables import always_iterable

if TYPE_CHECKING:
    from collections.abc import Callable

    from eventkit import Event

    from utilities.types import Coroutine1, MaybeCoroutine1, MaybeIterable, TCallable


def add_listener(
    event: Event,
    listener: Callable[..., MaybeCoroutine1[None]],
    /,
    *,
    error: Callable[[Event, Exception], MaybeCoroutine1[None]] | None = None,
    error_ignore: type[Exception] | tuple[type[Exception], ...] | None = None,
    error_decorators: MaybeIterable[Callable[[TCallable], TCallable]] | None = None,
    done: Callable[..., Any] | None = None,
    keep_ref: bool = False,
) -> Event:
    """Connect a listener to an event."""
    if error is None:
        listener_use = listener
    elif (not iscoroutinefunction(listener)) and iscoroutinefunction(error):
        raise ValueError
    elif not iscoroutinefunction(listener):
        listener_typed = cast("Callable[..., None]", listener)
        error_typed = cast("Callable[[Event, Exception], None]", error)

        @wraps(listener)
        def listener_sync(*args: Any, **kwargs: Any) -> None:
            try:
                return listener_typed(*args, **kwargs)
            except Exception as exc:  # noqa: BLE001
                if (error_ignore is not None) and isinstance(exc, error_ignore):
                    return None
                return error_typed(event, exc)

        listener_use = listener_sync
    else:
        listener_typed = cast("Callable[..., Coroutine1[None]]", listener)

        @wraps(listener)
        async def listener_async(*args: Any, **kwargs: Any) -> None:
            try:
                return await listener_typed(*args, **kwargs)
            except Exception as exc:  # noqa: BLE001
                if (error_ignore is not None) and isinstance(exc, error_ignore):
                    return None
                if iscoroutinefunction(error):
                    error_typed = cast(
                        "Callable[[Event, Exception], Coroutine1[None]]", error
                    )
                    return await error_typed(event, exc)
                error_typed = cast("Callable[[Event, Exception], None]", error)
                return error_typed(event, exc)

        listener_use = listener_async

    if error_decorators is not None:
        listener_use = apply_decorators(
            listener_use, *always_iterable(error_decorators)
        )

    return event.connect(listener_use, done=done, keep_ref=keep_ref)


__all__ = ["add_listener"]
