from __future__ import annotations

from asyncio import run, sleep
from contextlib import _GeneratorContextManager, contextmanager
from logging import getLogger
from signal import SIGINT, getsignal, signal
from typing import TYPE_CHECKING

from utilities.logging import setup_logging
from utilities.pathlib import get_repo_root

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator
    from types import FrameType


_LOGGER = getLogger(__name__)


def enhanced_context_manager[**P, T_co](
    func: Callable[P, Iterator[T_co]], /
) -> Callable[P, _GeneratorContextManager[T_co]]:
    make_cm = contextmanager(func)

    @contextmanager
    def wrapped(*args: P.args, **kwargs: P.kwargs) -> Iterator[T_co]:
        orig_sigint = getsignal(SIGINT)
        cm = make_cm(*args, **kwargs)

        def handler(signum: int, frame: FrameType | None) -> None:
            _ = (signum, frame)
            _ = cm.__exit__(None, None, None)

        _ = signal(SIGINT, handler)
        try:
            with cm as value:
                yield value
        finally:
            _ = signal(SIGINT, orig_sigint)

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
