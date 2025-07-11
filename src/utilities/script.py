from __future__ import annotations

from asyncio import run, sleep
from contextlib import _GeneratorContextManager, contextmanager
from logging import getLogger
from signal import SIGABRT, SIGFPE, SIGILL, SIGINT, SIGSEGV, SIGTERM, getsignal, signal
from typing import TYPE_CHECKING, Any

from utilities.logging import setup_logging
from utilities.pathlib import get_repo_root
from utilities.random import bernoulli

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator
    from signal import _HANDLER, _SIGNUM
    from types import FrameType


_LOGGER = getLogger(__name__)


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
) -> Callable[P, _GeneratorContextManager[T_co]]:
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
        _ = gcm.__exit__(None, None, None)
        if callable(orig_handler):
            orig_handler(signum, frame)
        else:
            pass

    return new_handler


@enhanced_context_manager
def context() -> Iterator[None]:
    path = get_repo_root().joinpath("dummy")
    path.touch()
    try:
        yield
    finally:
        path.unlink(missing_ok=True)


async def main() -> None:
    setup_logging(logger=_LOGGER, files_dir=".logs")
    _LOGGER.info("starting...")
    n = 9
    _LOGGER.info("sleeping for %d...", n)
    with context():
        await sleep(n / 2)
        if bernoulli():
            msg = "!!!"
            raise ValueError(msg)
        _LOGGER.info("safe...")
        await sleep(n / 2)
    _LOGGER.info("finished")


if __name__ == "__main__":
    run(main())
