from __future__ import annotations

from asyncio import run, sleep
from contextlib import _GeneratorContextManager, asynccontextmanager, contextmanager
from dataclasses import dataclass
from functools import wraps
from logging import getLogger
from signal import SIGINT, default_int_handler, getsignal, signal
from typing import TYPE_CHECKING, Any, TypeVar, override

from utilities.logging import setup_logging
from utilities.pathlib import get_repo_root

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Callable, Generator, Iterator
    from signal import _HANDLER, _SIGNUM
    from types import FrameType, TracebackType


_LOGGER = getLogger(__name__)
_G_co = TypeVar("_G_co", covariant=True)


class EnhancedContextManager(_GeneratorContextManager[_G_co]):
    sigint: _HANDLER | None

    @override
    def __init__(
        self,
        func: Callable[..., Generator[_G_co, None, None]],
        args: tuple[Any, ...],
        kwds: dict[str, Any],
    ) -> None:
        super().__init__(func, args, kwds)
        self.sigint = None

    @override
    def __enter__(self) -> _G_co:
        self.sigint = getsignal(SIGINT)

        def handler(signum: int, frame: FrameType | None) -> None:
            _ = (signum, frame)
            _ = self.__exit__(None, None, None)

        _ = signal(SIGINT, handler)
        return super().__enter__()

    @override
    def __exit__(
        self,
        typ: type[BaseException] | None,
        value: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool | None:
        _ = signal(SIGINT, self.sigint)
        return super().__exit__(typ, value, traceback)


T_co = TypeVar("T_co", covariant=True)


def enhanced_context_manager[**P, T_co](
    func: Callable[P, Iterator[T_co]], /
) -> Callable[P, EnhancedContextManager[T_co]]:
    @wraps(func)
    def wrapped(*args: Any, **kwds: Any) -> EnhancedContextManager[Any]:
        return EnhancedContextManager(func, args, kwds)

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
    with context() as asdf, context() as asdf:
        await sleep(5)
    _LOGGER.info("finished")


if __name__ == "__main__":
    run(main())
