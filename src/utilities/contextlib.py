from __future__ import annotations

import re
from asyncio import get_event_loop
from contextlib import (
    _AsyncGeneratorContextManager,
    _GeneratorContextManager,
    asynccontextmanager,
    contextmanager,
)
from functools import partial
from signal import SIGABRT, SIGFPE, SIGILL, SIGINT, SIGSEGV, SIGTERM, getsignal, signal
from typing import TYPE_CHECKING, Any, cast, overload

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
        sigabrt0 = sigfpe0 = sigill0 = sigint0 = sigsegv0 = sigterm0 = None
        gcm = make_gcm(*args, **kwargs)
        if sigabrt:
            sigabrt0 = _swap_handler(SIGABRT, gcm)
        if sigfpe:
            sigfpe0 = _swap_handler(SIGFPE, gcm)
        if sigill:
            sigill0 = _swap_handler(SIGILL, gcm)
        if sigint:
            sigint0 = _swap_handler(SIGINT, gcm)
        if sigsegv:
            sigsegv0 = _swap_handler(SIGSEGV, gcm)
        if sigterm:
            sigterm0 = _swap_handler(SIGTERM, gcm)
        try:
            with gcm as value:
                yield value
        finally:
            if sigabrt:
                _ = signal(SIGABRT, sigabrt0)
            if sigfpe:
                _ = signal(SIGFPE, sigfpe0)
            if sigill:
                _ = signal(SIGILL, sigill0)
            if sigint:
                _ = signal(SIGINT, sigint0)
            if sigsegv:
                _ = signal(SIGSEGV, sigsegv0)
            if sigterm:
                _ = signal(SIGTERM, sigterm0)

    return wrapped


def _swap_handler(
    signum: _SIGNUM, gcm: _GeneratorContextManager[Any, None, None], /
) -> _HANDLER:
    orig_handler = getsignal(signum)
    new_handler = _make_handler(signum, gcm)
    _ = signal(signum, new_handler)
    return orig_handler


def _make_handler(
    signum: _SIGNUM, gcm: _GeneratorContextManager[Any, None, None], /
) -> Callable[[int, FrameType | None], None]:
    orig_handler = getsignal(signum)

    def new_handler(signum: int, frame: FrameType | None) -> None:
        _ = gcm.__exit__(None, None, None)  # pragma: no cover
        if callable(orig_handler):  # pragma: no cover
            orig_handler(signum, frame)

    return new_handler


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
        sigabrt0 = sigfpe0 = sigill0 = sigint0 = sigsegv0 = sigterm0 = None
        agcm = make_agcm(*args, **kwargs)
        if sigabrt:
            sigabrt0 = _swap_async_handler(SIGABRT, agcm)
        if sigfpe:
            sigfpe0 = _swap_async_handler(SIGFPE, agcm)
        if sigill:
            sigill0 = _swap_async_handler(SIGILL, agcm)
        if sigint:
            sigint0 = _swap_async_handler(SIGINT, agcm)
        if sigsegv:
            sigsegv0 = _swap_async_handler(SIGSEGV, agcm)
        if sigterm:
            sigterm0 = _swap_async_handler(SIGTERM, agcm)
        try:
            async with agcm as value:
                yield value
        finally:
            if sigabrt:
                _ = signal(SIGABRT, sigabrt0)
            if sigfpe:
                _ = signal(SIGFPE, sigfpe0)
            if sigill:
                _ = signal(SIGILL, sigill0)
            if sigint:
                _ = signal(SIGINT, sigint0)
            if sigsegv:
                _ = signal(SIGSEGV, sigsegv0)
            if sigterm:
                _ = signal(SIGTERM, sigterm0)

    return wrapped


def _swap_async_handler(
    signum: _SIGNUM, agcm: _AsyncGeneratorContextManager[Any, None], /
) -> _HANDLER:
    orig_handler = getsignal(signum)
    new_handler = _make_async_handler(signum, agcm)
    _ = signal(signum, new_handler)
    return orig_handler


def _make_async_handler(
    signum: _SIGNUM, agcm: _AsyncGeneratorContextManager[Any, None], /
) -> Callable[[int, FrameType | None], None]:
    orig_handler = getsignal(signum)

    def new_handler(signum: int, frame: FrameType | None) -> None:
        loop = get_event_loop()  # pragma: no cover
        _ = loop.run_until_complete(  # pragma: no cover
            agcm.__aexit__(None, None, None)
        )
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
