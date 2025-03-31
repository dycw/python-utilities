from __future__ import annotations

from asyncio import iscoroutinefunction
from dataclasses import dataclass
from functools import wraps
from typing import TYPE_CHECKING, Any, assert_never, cast, override

from utilities.functions import apply_decorators
from utilities.iterables import always_iterable
from utilities.logging import get_logger

if TYPE_CHECKING:
    from collections.abc import Callable

    from eventkit import Event

    from utilities.types import (
        Coroutine1,
        LoggerOrName,
        MaybeCoroutine1,
        MaybeIterable,
        TCallable,
    )


def add_listener(
    event: Event,
    listener: Callable[..., MaybeCoroutine1[None]],
    /,
    *,
    error: Callable[[Event, BaseException], MaybeCoroutine1[None]] | None = None,
    ignore: type[BaseException] | tuple[type[BaseException], ...] | None = None,
    logger: LoggerOrName | None = None,
    decorators: MaybeIterable[Callable[[TCallable], TCallable]] | None = None,
    done: Callable[..., Any] | None = None,
    keep_ref: bool = False,
) -> Event:
    """Connect a listener to an event."""
    match error, bool(iscoroutinefunction(listener)):
        case None, False:
            listener_typed = cast("Callable[..., None]", listener)

            @wraps(listener)
            def listener_no_error_sync(*args: Any, **kwargs: Any) -> None:
                try:
                    listener_typed(*args, **kwargs)
                except Exception as exc:  # noqa: BLE001
                    if (ignore is not None) and isinstance(exc, ignore):
                        return
                    get_logger(logger=logger).exception("")

            listener_use = listener_no_error_sync

        case None, True:
            listener_typed = cast("Callable[..., Coroutine1[None]]", listener)

            @wraps(listener)
            async def listener_no_error_async(*args: Any, **kwargs: Any) -> None:
                try:
                    await listener_typed(*args, **kwargs)
                except Exception as exc:  # noqa: BLE001
                    if (ignore is not None) and isinstance(exc, ignore):
                        return
                    get_logger(logger=logger).exception("")

            listener_use = listener_no_error_async
        case _, _:
            match bool(iscoroutinefunction(listener)), bool(iscoroutinefunction(error)):
                case False, False:
                    listener_typed = cast("Callable[..., None]", listener)
                    error_typed = cast("Callable[[Event, Exception], None]", error)

                    @wraps(listener)
                    def listener_have_error_sync(*args: Any, **kwargs: Any) -> None:
                        try:
                            listener_typed(*args, **kwargs)
                        except Exception as exc:  # noqa: BLE001
                            if (ignore is not None) and isinstance(exc, ignore):
                                return
                            error_typed(event, exc)

                    listener_use = listener_have_error_sync
                case False, True:
                    listener_typed = cast("Callable[..., None]", listener)
                    error_typed = cast(
                        "Callable[[Event, Exception], Coroutine1[None]]", error
                    )
                    raise AddListenerError(listener=listener_typed, error=error_typed)
                case True, _:
                    listener_typed = cast("Callable[..., Coroutine1[None]]", listener)

                    @wraps(listener)
                    async def listener_have_error_async(
                        *args: Any, **kwargs: Any
                    ) -> None:
                        try:
                            await listener_typed(*args, **kwargs)
                        except Exception as exc:  # noqa: BLE001
                            if (ignore is not None) and isinstance(exc, ignore):
                                return None
                            if iscoroutinefunction(error):
                                error_typed = cast(
                                    "Callable[[Event, Exception], Coroutine1[None]]",
                                    error,
                                )
                                return await error_typed(event, exc)
                            error_typed = cast(
                                "Callable[[Event, Exception], None]", error
                            )
                            error_typed(event, exc)

                    listener_use = listener_have_error_async
                case _ as never:
                    assert_never(never)
        case _ as never:
            assert_never(never)

    if decorators is not None:
        listener_use = apply_decorators(listener_use, *always_iterable(decorators))

    return event.connect(listener_use, done=done, keep_ref=keep_ref)


@dataclass(kw_only=True, slots=True)
class AddListenerError(Exception):
    listener: Callable[..., None]
    error: Callable[[Event, Exception], Coroutine1[None]]

    @override
    def __str__(self) -> str:
        return f"Synchronous listener {self.listener} cannot be paired with an asynchronous error handler {self.error}"


__all__ = ["AddListenerError", "add_listener"]
