from __future__ import annotations

from asyncio import run, sleep
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING

from utilities.logging import setup_logging
from utilities.pathlib import get_repo_root

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
from logging import getLogger

_LOGGER = getLogger(__name__)


@asynccontextmanager
async def context() -> AsyncIterator[None]:
    path = get_repo_root().joinpath("dummy")
    path.touch()
    yield
    path.unlink(missing_ok=True)


async def main() -> None:
    setup_logging(logger=_LOGGER, files_dir=".logs")
    _LOGGER.info("starting...")
    _LOGGER.info("sleeping for 5...")
    async with context():
        await sleep(5)
    _LOGGER.info("finished")


if __name__ == "__main__":
    run(main())
