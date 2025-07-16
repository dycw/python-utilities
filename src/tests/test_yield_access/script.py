from __future__ import annotations

from asyncio import run
from logging import getLogger
from random import randint
from typing import TYPE_CHECKING

from redis.asyncio import Redis

from utilities.asyncio import sleep_td
from utilities.logging import setup_logging
from utilities.pathlib import get_repo_root
from utilities.pottery import extend_lock, try_yield_coroutine_looper
from utilities.whenever import SECOND

if TYPE_CHECKING:
    from pottery import AIORedlock

_LOGGER = getLogger(__name__)


async def script(*, lock: AIORedlock | None = None) -> None:
    total = 1000
    fail = 30
    success = 3
    while True:
        n = randint(0, total)
        if n < fail:
            _LOGGER.info("n = %d; failing...", n)
            msg = f"n = {n}; failure"
            raise ValueError(msg)
        if fail <= n < (fail + success):
            _LOGGER.info("n = %d; succeeding...", n)
            return
        _LOGGER.info("n = %d", n)
        await extend_lock(lock=lock)
        await sleep_td(SECOND / 3)


async def service() -> None:
    redis = Redis()
    async with try_yield_coroutine_looper(
        redis,
        "utilities-test",
        num=1,
        timeout_release=5 * SECOND,
        logger=_LOGGER,
        sleep_error=4 * SECOND,
    ) as looper:
        if looper is not None:
            result = await looper(script, lock=looper.lock)
            _LOGGER.info("script %s", "succeeded" if result else "failed")


def main() -> None:
    setup_logging(logger=_LOGGER, files_dir=get_repo_root().joinpath(".logs"))
    run(service())


if __name__ == "__main__":
    main()
