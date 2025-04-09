from __future__ import annotations

from asyncio import iscoroutinefunction
from dataclasses import dataclass
from functools import wraps
from typing import (
    TYPE_CHECKING,
    Any,
    Generic,
    Self,
    TypeVar,
    assert_never,
    cast,
    override,
)

from eventkit import Event

from utilities.functions import apply_decorators
from utilities.iterables import always_iterable
from utilities.logging import get_logger
from utilities.types import TCallable, TCallableMaybeCoroutine1None

if TYPE_CHECKING:
    from collections.abc import Callable

    from utilities.types import Coroutine1, LoggerOrName, MaybeCoroutine1, MaybeIterable


_TEvent = TypeVar("_TEvent", bound=Event)


##


def add_listener(
    event: _TEvent,
    listener: Callable[..., MaybeCoroutine1[None]],
    /,
    *,
    error: Callable[[Event, BaseException], MaybeCoroutine1[None]] | None = None,
    ignore: type[BaseException] | tuple[type[BaseException], ...] | None = None,
    logger: LoggerOrName | None = None,
    decorators: MaybeIterable[Callable[[TCallable], TCallable]] | None = None,
    done: Callable[..., Any] | None = None,
    keep_ref: bool = False,
) -> _TEvent:
    """Connect a listener to an event."""
    lifted = _lift_listener(
        listener,
        event,
        error=error,
        ignore=ignore,
        logger=logger,
        decorators=decorators,
    )
    return cast("_TEvent", event.connect(lifted, done=done, keep_ref=keep_ref))


@dataclass(kw_only=True, slots=True)
class AddListenerError(Exception):
    listener: Callable[..., None]
    error: Callable[[Event, Exception], Coroutine1[None]]

    @override
    def __str__(self) -> str:
        return f"Synchronous listener {self.listener} cannot be paired with an asynchronous error handler {self.error}"


##


class EnhancedEvent(Event, Generic[TCallableMaybeCoroutine1None]):
    """An enhanced version of `Event`."""

    @override
    def connect(
        self,
        listener: TCallableMaybeCoroutine1None,
        error: Callable[[Self, BaseException], MaybeCoroutine1[None]] | None = None,
        done: Callable[[Self], MaybeCoroutine1[None]] | None = None,
        keep_ref: bool = False,
        *,
        ignore: type[BaseException] | tuple[type[BaseException], ...] | None = None,
        logger: LoggerOrName | None = None,
        decorators: MaybeIterable[Callable[[TCallable], TCallable]] | None = None,
    ) -> Self:
        lifted = _lift_listener(
            listener,
            self,
            error=cast(
                "Callable[[Event, BaseException], MaybeCoroutine1[None]] | None", error
            ),
            ignore=ignore,
            logger=logger,
            decorators=decorators,
        )
        return cast(
            "Self", super().connect(lifted, error=error, done=done, keep_ref=keep_ref)
        )


##


def _lift_listener(
    listener: Callable[..., MaybeCoroutine1[None]],
    event: Event,
    /,
    *,
    error: Callable[[Event, BaseException], MaybeCoroutine1[None]] | None = None,
    ignore: type[BaseException] | tuple[type[BaseException], ...] | None = None,
    logger: LoggerOrName | None = None,
    decorators: MaybeIterable[Callable[[TCallable], TCallable]] | None = None,
) -> Callable[..., MaybeCoroutine1[None]]:
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

            lifted = listener_no_error_sync

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

            lifted = listener_no_error_async
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

                    lifted = listener_have_error_sync
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

                    lifted = listener_have_error_async
                case _ as never:
                    assert_never(never)
        case _ as never:
            assert_never(never)

    if decorators is not None:
        lifted = apply_decorators(lifted, *always_iterable(decorators))
    return lifted


__all__ = ["AddListenerError", "EnhancedEvent", "add_listener"]
