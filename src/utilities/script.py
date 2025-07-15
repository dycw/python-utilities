from __future__ import annotations

from asyncio import run
from logging import getLogger
from typing import TYPE_CHECKING

from redis.asyncio import Redis

from utilities.asyncio import sleep_td
from utilities.logging import setup_logging
from utilities.pathlib import get_repo_root
from utilities.pottery import extend_lock, try_yield_coroutine_looper
from utilities.random import SYSTEM_RANDOM
from utilities.whenever import SECOND

if TYPE_CHECKING:
    from pottery import AIORedlock

_LOGGER = getLogger(__name__)


async def script(*, lock: AIORedlock | None = None) -> None:
    while True:
        _LOGGER.info("%d", SYSTEM_RANDOM.randint(0, 100))
        await extend_lock(lock=lock)
        await sleep_td(SECOND)


async def service() -> None:
    redis = Redis()
    async with try_yield_coroutine_looper(
        redis, "utilities-test", timeout_release=10 * SECOND, logger=_LOGGER
    ) as looper:
        if looper is not None:
            await looper(script, lock=looper.lock)


def main() -> None:
    setup_logging(logger=_LOGGER, files_dir=get_repo_root().joinpath(".logs"))
    run(service())


if __name__ == "__main__":
    main()
