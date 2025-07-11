from __future__ import annotations

import re
from asyncio import create_task, get_event_loop
from contextlib import (
    _AsyncGeneratorContextManager,
    _GeneratorContextManager,
    asynccontextmanager,
    contextmanager,
)
from functools import partial
from signal import SIGABRT, SIGFPE, SIGILL, SIGINT, SIGSEGV, SIGTERM, getsignal, signal
from typing import TYPE_CHECKING, Any, assert_never, cast, overload

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Callable, Iterator
    from signal import _HANDLER, _SIGNUM
    from types import FrameType


@overload
def enhanced_context_manager[**P, T_co](
    func: Callable[P, Iterator[T_co]],
    /,
    *,
    sigabrt: bool = True,
    sigfpe: bool = True,
    sigill: bool = True,
    sigint: bool = True,
    sigsegv: bool = True,
    sigterm: bool = True,
) -> Callable[P, _GeneratorContextManager[T_co]]: ...
@overload
def enhanced_context_manager[**P, T_co](
    func: None = None,
    /,
    *,
    sigabrt: bool = True,
    sigfpe: bool = True,
    sigill: bool = True,
    sigint: bool = True,
    sigsegv: bool = True,
    sigterm: bool = True,
) -> Callable[
    [Callable[P, Iterator[T_co]]], Callable[P, _GeneratorContextManager[T_co]]
]: ...
def enhanced_context_manager[**P, T_co](
    func: Callable[P, Iterator[T_co]] | None = None,
    /,
    *,
    sigabrt: bool = True,
    sigfpe: bool = True,
    sigill: bool = True,
    sigint: bool = True,
    sigsegv: bool = True,
    sigterm: bool = True,
) -> (
    Callable[P, _GeneratorContextManager[T_co]]
    | Callable[
        [Callable[P, Iterator[T_co]]], Callable[P, _GeneratorContextManager[T_co]]
    ]
):
    if func is None:
        result = partial(
            enhanced_context_manager,
            sigabrt=sigabrt,
            sigfpe=sigfpe,
            sigill=sigill,
            sigint=sigint,
            sigsegv=sigsegv,
            sigterm=sigterm,
        )
        return cast(
            "Callable[[Callable[P, Iterator[T_co]]], Callable[P, _GeneratorContextManager[T_co]]]",
            result,
        )
    make_gcm = contextmanager(func)

    @contextmanager
    def wrapped(*args: P.args, **kwargs: P.kwargs) -> Iterator[T_co]:
        gcm = make_gcm(*args, **kwargs)
        sigabrt0 = _swap_handler(SIGABRT, gcm) if sigabrt else None
        sigfpe0 = _swap_handler(SIGFPE, gcm) if sigfpe else None
        sigill0 = _swap_handler(SIGILL, gcm) if sigill else None
        sigint0 = _swap_handler(SIGINT, gcm) if sigint else None
        sigsegv0 = _swap_handler(SIGSEGV, gcm) if sigsegv else None
        sigterm0 = _swap_handler(SIGTERM, gcm) if sigterm else None
        try:
            with gcm as value:
                yield value
        finally:
            _ = signal(SIGABRT, sigabrt0) if sigabrt else None
            _ = signal(SIGFPE, sigfpe0) if sigfpe else None
            _ = signal(SIGILL, sigill0) if sigill else None
            _ = signal(SIGINT, sigint0) if sigint else None
            _ = signal(SIGSEGV, sigsegv0) if sigsegv else None
            _ = signal(SIGTERM, sigterm0) if sigterm else None

    return wrapped


@overload
def enhanced_async_context_manager[**P, T_co](
    func: Callable[P, AsyncIterator[T_co]],
    /,
    *,
    sigabrt: bool = True,
    sigfpe: bool = True,
    sigill: bool = True,
    sigint: bool = True,
    sigsegv: bool = True,
    sigterm: bool = True,
) -> Callable[P, _AsyncGeneratorContextManager[T_co]]: ...
@overload
def enhanced_async_context_manager[**P, T_co](
    func: None = None,
    /,
    *,
    sigabrt: bool = True,
    sigfpe: bool = True,
    sigill: bool = True,
    sigint: bool = True,
    sigsegv: bool = True,
    sigterm: bool = True,
) -> Callable[
    [Callable[P, AsyncIterator[T_co]]], Callable[P, _AsyncGeneratorContextManager[T_co]]
]: ...
def enhanced_async_context_manager[**P, T_co](
    func: Callable[P, AsyncIterator[T_co]] | None = None,
    /,
    *,
    sigabrt: bool = True,
    sigfpe: bool = True,
    sigill: bool = True,
    sigint: bool = True,
    sigsegv: bool = True,
    sigterm: bool = True,
) -> (
    Callable[P, _AsyncGeneratorContextManager[T_co]]
    | Callable[
        [Callable[P, AsyncIterator[T_co]]],
        Callable[P, _AsyncGeneratorContextManager[T_co]],
    ]
):
    if func is None:
        result = partial(
            enhanced_async_context_manager,
            sigabrt=sigabrt,
            sigfpe=sigfpe,
            sigill=sigill,
            sigint=sigint,
            sigsegv=sigsegv,
            sigterm=sigterm,
        )
        return cast(
            "Callable[[Callable[P, AsyncIterator[T_co]]], Callable[P, _AsyncGeneratorContextManager[T_co]]]",
            result,
        )
    make_agcm = asynccontextmanager(func)

    @asynccontextmanager
    async def wrapped(*args: P.args, **kwargs: P.kwargs) -> AsyncIterator[T_co]:
        agcm = make_agcm(*args, **kwargs)
        sigabrt0 = _swap_handler(SIGABRT, agcm) if sigabrt else None
        sigfpe0 = _swap_handler(SIGFPE, agcm) if sigfpe else None
        sigill0 = _swap_handler(SIGILL, agcm) if sigill else None
        sigint0 = _swap_handler(SIGINT, agcm) if sigint else None
        sigsegv0 = _swap_handler(SIGSEGV, agcm) if sigsegv else None
        sigterm0 = _swap_handler(SIGTERM, agcm) if sigterm else None
        try:
            async with agcm as value:
                yield value
        finally:
            _ = signal(SIGABRT, sigabrt0) if sigabrt else None
            _ = signal(SIGFPE, sigfpe0) if sigfpe else None
            _ = signal(SIGILL, sigill0) if sigill else None
            _ = signal(SIGINT, sigint0) if sigint else None
            _ = signal(SIGSEGV, sigsegv0) if sigsegv else None
            _ = signal(SIGTERM, sigterm0) if sigterm else None

    return wrapped


def _swap_handler(
    signum: _SIGNUM,
    obj: _GeneratorContextManager[Any, None, None]
    | _AsyncGeneratorContextManager[Any, None],
    /,
) -> _HANDLER:
    orig_handler = getsignal(signum)
    new_handler = _make_handler(signum, obj)
    _ = signal(signum, new_handler)
    return orig_handler


def _make_handler(
    signum: _SIGNUM,
    obj: _GeneratorContextManager[Any, None, None]
    | _AsyncGeneratorContextManager[Any, None],
    /,
) -> Callable[[int, FrameType | None], None]:
    orig_handler = getsignal(signum)

    def new_handler(signum: int, frame: FrameType | None) -> None:
        match obj:  # pragma: no cover
            case _GeneratorContextManager() as gcm:
                _ = gcm.__exit__(None, None, None)
            case _AsyncGeneratorContextManager() as agcm:
                loop = get_event_loop()
                _ = loop.call_soon_threadsafe(
                    create_task, agcm.__aexit__(None, None, None)
                )
            case _ as never:
                assert_never(never)
        if callable(orig_handler):  # pragma: no cover
            orig_handler(signum, frame)

    return new_handler


##


_SUPER_OBJECT_HAS_NO_ATTRIBUTE = re.compile(r"'super' object has no attribute '\w+'")


@contextmanager
def suppress_super_object_attribute_error() -> Iterator[None]:
    """Suppress the super() attribute error, for mix-ins."""
    try:
        yield
    except AttributeError as error:
        if not _SUPER_OBJECT_HAS_NO_ATTRIBUTE.search(error.args[0]):
            raise


__all__ = [
    "enhanced_async_context_manager",
    "enhanced_context_manager",
    "suppress_super_object_attribute_error",
]
