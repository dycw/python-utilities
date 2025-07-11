from __future__ import annotations

from asyncio import run, sleep
from contextlib import _GeneratorContextManager, contextmanager
from logging import getLogger
from signal import SIGABRT, SIGFPE, SIGILL, SIGINT, SIGSEGV, SIGTERM, getsignal, signal
from typing import TYPE_CHECKING

from utilities.logging import setup_logging
from utilities.pathlib import get_repo_root

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator
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
    make_cm = contextmanager(func)

    @contextmanager
    def wrapped(*args: P.args, **kwargs: P.kwargs) -> Iterator[T_co]:
        sigabrt0 = sigfpe0 = sigill0 = sigint0 = sigsegv0 = sigterm0 = None
        cm = make_cm(*args, **kwargs)

        def handler(signum: int, frame: FrameType | None) -> None:
            _ = (signum, frame)
            _ = cm.__exit__(None, None, None)

        if sigabrt:
            sigabrt0, _ = getsignal(SIGABRT), signal(SIGABRT, handler)
        if sigfpe:
            sigfpe0, _ = getsignal(SIGFPE), signal(SIGFPE, handler)
        if sigill:
            sigill0, _ = getsignal(SIGILL), signal(SIGILL, handler)
        if sigint:
            sigint0, _ = getsignal(SIGINT), signal(SIGINT, handler)
        if sigsegv:
            sigsegv0, _ = getsignal(SIGSEGV), signal(SIGSEGV, handler)
        if sigterm:
            sigterm0, _ = getsignal(SIGTERM), signal(SIGTERM, handler)
        try:
            with cm as value:
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


@enhanced_context_manager
def context() -> Iterator[None]:
    path = get_repo_root().joinpath("dummy")
    path.touch()
    yield
    path.unlink(missing_ok=True)


async def main() -> None:
    setup_logging(logger=_LOGGER, files_dir=".logs")
    _LOGGER.info("starting...")
    _LOGGER.info("sleeping for 5...")
    with context():
        await sleep(8)
    _LOGGER.info("finished")


if __name__ == "__main__":
    run(main())
