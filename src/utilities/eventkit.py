from __future__ import annotations

from dataclasses import dataclass
from functools import wraps
from inspect import iscoroutinefunction
from typing import TYPE_CHECKING, Any, Self, assert_never, cast, override

from eventkit import (
    Constant,
    Count,
    DropWhile,
    Enumerate,
    Event,
    Filter,
    Fork,
    Iterate,
    Map,
    Pack,
    Partial,
    PartialRight,
    Pluck,
    Skip,
    Star,
    Take,
    TakeUntil,
    TakeWhile,
    Timestamp,
)

from utilities.functions import apply_decorators
from utilities.iterables import always_iterable
from utilities.logging import get_logger

if TYPE_CHECKING:
    from collections.abc import Callable

    from utilities.types import Coro, LoggerOrName, MaybeCoro, MaybeIterable, TypeLike


##


def add_listener[E: Event, F: Callable](
    event: E,
    listener: Callable[..., MaybeCoro[None]],
    /,
    *,
    error: Callable[[Event, BaseException], MaybeCoro[None]] | None = None,
    ignore: TypeLike[BaseException] | None = None,
    logger: LoggerOrName | None = None,
    decorators: MaybeIterable[Callable[[F], F]] | None = None,
    done: Callable[..., MaybeCoro[None]] | None = None,
    keep_ref: bool = False,
) -> E:
    """Connect a listener to an event."""
    lifted = lift_listener(
        listener,
        event,
        error=error,
        ignore=ignore,
        logger=logger,
        decorators=decorators,
    )
    return cast("E", event.connect(lifted, done=done, keep_ref=keep_ref))


##


@dataclass(repr=False, kw_only=True)
class LiftedEvent[F: Callable[..., MaybeCoro[None]]]:
    """A lifted version of `Event`."""

    event: Event

    def name(self) -> str:
        return self.event.name()  # pragma: no cover

    def done(self) -> bool:
        return self.event.done()  # pragma: no cover

    def set_done(self) -> None:
        self.event.set_done()  # pragma: no cover

    def value(self) -> Any:
        return self.event.value()  # pragma: no cover

    def connect[F2: Callable](
        self,
        listener: F,
        /,
        *,
        error: Callable[[Event, BaseException], MaybeCoro[None]] | None = None,
        ignore: TypeLike[BaseException] | None = None,
        logger: LoggerOrName | None = None,
        decorators: MaybeIterable[Callable[[F2], F2]] | None = None,
        done: Callable[..., MaybeCoro[None]] | None = None,
        keep_ref: bool = False,
    ) -> Event:
        return add_listener(
            self.event,
            listener,
            error=error,
            ignore=ignore,
            logger=logger,
            decorators=decorators,
            done=done,
            keep_ref=keep_ref,
        )

    def disconnect(
        self, listener: Any, /, *, error: Any = None, done: Any = None
    ) -> Any:
        return self.event.disconnect(  # pragma: no cover
            listener, error=error, done=done
        )

    def disconnect_obj(self, obj: Any, /) -> None:
        self.event.disconnect_obj(obj)  # pragma: no cover

    def emit(self, *args: Any) -> None:
        self.event.emit(*args)  # pragma: no cover

    def emit_threadsafe(self, *args: Any) -> None:
        self.event.emit_threadsafe(*args)  # pragma: no cover

    def clear(self) -> None:
        self.event.clear()  # pragma: no cover

    def run(self) -> list[Any]:
        return self.event.run()  # pragma: no cover

    def pipe(self, *targets: Event) -> Event:
        return self.event.pipe(*targets)  # pragma: no cover

    def fork(self, *targets: Event) -> Fork:
        return self.event.fork(*targets)  # pragma: no cover

    def set_source(self, source: Any, /) -> None:
        self.event.set_source(source)  # pragma: no cover

    def _onFinalize(self, ref: Any) -> None:  # noqa: N802
        self.event._onFinalize(ref)  # noqa: SLF001 # pragma: no cover

    async def aiter(self, *, skip_to_last: bool = False, tuples: bool = False) -> Any:
        async for i in self.event.aiter(  # pragma: no cover
            skip_to_last=skip_to_last, tuples=tuples
        ):
            yield i

    __iadd__ = connect
    __isub__ = disconnect
    __call__ = emit
    __or__ = pipe

    @override
    def __repr__(self) -> str:
        return self.event.__repr__()  # pragma: no cover

    def __len__(self) -> int:
        return self.event.__len__()  # pragma: no cover

    def __bool__(self) -> bool:
        return self.event.__bool__()  # pragma: no cover

    def __getitem__(self, fork_targets: Any, /) -> Fork:
        return self.event.__getitem__(fork_targets)  # pragma: no cover

    def __await__(self) -> Any:
        return self.event.__await__()  # pragma: no cover

    __aiter__ = aiter

    def __contains__(self, c: Any, /) -> bool:
        return self.event.__contains__(c)  # pragma: no cover

    @override
    def __reduce__(self) -> Any:
        return self.event.__reduce__()  # pragma: no cover

    def filter(self, *, predicate: Any = bool) -> Filter:
        return self.event.filter(predicate=predicate)  # pragma: no cover

    def skip(self, *, count: int = 1) -> Skip:
        return self.event.skip(count=count)  # pragma: no cover

    def take(self, *, count: int = 1) -> Take:
        return self.event.take(count=count)  # pragma: no cover

    def takewhile(self, *, predicate: Any = bool) -> TakeWhile:
        return self.event.takewhile(predicate=predicate)  # pragma: no cover

    def dropwhile(self, *, predicate: Any = lambda x: not x) -> DropWhile:  # pyright: ignore[reportUnknownLambdaType]
        return self.event.dropwhile(predicate=predicate)  # pragma: no cover

    def takeuntil(self, notifier: Event, /) -> TakeUntil:
        return self.event.takeuntil(notifier)  # pragma: no cover

    def constant(self, constant: Any, /) -> Constant:
        return self.event.constant(constant)  # pragma: no cover

    def iterate(self, it: Any, /) -> Iterate:
        return self.event.iterate(it)  # pragma: no cover

    def count(self, *, start: int = 0, step: int = 1) -> Count:
        return self.event.count(start=start, step=step)  # pragma: no cover

    def enumerate(self, *, start: int = 0, step: int = 1) -> Enumerate:
        return self.event.enumerate(start=start, step=step)  # pragma: no cover

    def timestamp(self) -> Timestamp:
        return self.event.timestamp()  # pragma: no cover

    def partial(self, *left_args: Any) -> Partial:
        return self.event.partial(*left_args)  # pragma: no cover

    def partial_right(self, *right_args: Any) -> PartialRight:
        return self.event.partial_right(*right_args)  # pragma: no cover

    def star(self) -> Star:
        return self.event.star()  # pragma: no cover

    def pack(self) -> Pack:
        return self.event.pack()  # pragma: no cover

    def pluck(self, *selections: int | str) -> Pluck:
        return self.event.pluck(*selections)  # pragma: no cover

    def map(
        self,
        func: Any,
        /,
        *,
        timeout: float | None = None,
        ordered: bool = True,
        task_limit: int | None = None,
    ) -> Map:
        return self.event.map(  # pragma: no cover
            func, timeout=timeout, ordered=ordered, task_limit=task_limit
        )


##


class TypedEvent[F: Callable[..., MaybeCoro[None]]](Event):
    """A typed version of `Event`."""

    @override
    def connect[F2: Callable](
        self,
        listener: F,
        error: Callable[[Self, BaseException], MaybeCoro[None]] | None = None,
        done: Callable[[Self], MaybeCoro[None]] | None = None,
        keep_ref: bool = False,
        *,
        ignore: TypeLike[BaseException] | None = None,
        logger: LoggerOrName | None = None,
        decorators: MaybeIterable[Callable[[F2], F2]] | None = None,
    ) -> Self:
        lifted = lift_listener(
            listener,
            self,
            error=cast(
                "Callable[[Event, BaseException], MaybeCoro[None]] | None", error
            ),
            ignore=ignore,
            logger=logger,
            decorators=decorators,
        )
        return cast(
            "Self", super().connect(lifted, error=error, done=done, keep_ref=keep_ref)
        )


##


def lift_listener[F1: Callable[..., MaybeCoro[None]], F2: Callable](
    listener: F1,
    event: Event,
    /,
    *,
    error: Callable[[Event, BaseException], MaybeCoro[None]] | None = None,
    ignore: TypeLike[BaseException] | None = None,
    logger: LoggerOrName | None = None,
    decorators: MaybeIterable[Callable[[F2], F2]] | None = None,
) -> F1:
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
            listener_typed = cast("Callable[..., Coro[None]]", listener)

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
                        "Callable[[Event, Exception], Coro[None]]", error
                    )
                    raise LiftListenerError(listener=listener_typed, error=error_typed)
                case True, _:
                    listener_typed = cast("Callable[..., Coro[None]]", listener)

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
                                    "Callable[[Event, Exception], Coro[None]]", error
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
    return cast("F1", lifted)


@dataclass(kw_only=True, slots=True)
class LiftListenerError(Exception):
    listener: Callable[..., None]
    error: Callable[[Event, Exception], Coro[None]]

    @override
    def __str__(self) -> str:
        return f"Synchronous listener {self.listener} cannot be paired with an asynchronous error handler {self.error}"


__all__ = [
    "LiftListenerError",
    "LiftedEvent",
    "TypedEvent",
    "add_listener",
    "lift_listener",
]
