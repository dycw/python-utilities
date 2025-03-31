from __future__ import annotations

from asyncio import CancelledError, run, sleep
from contextlib import suppress
from logging import getLogger
from typing import override

from utilities.asyncio import AsyncLoopingService, AsyncService
from utilities.logging import basic_config
from utilities.random import SYSTEM_RANDOM

_LOGGER = getLogger(__name__)


class Service(AsyncService):
    @override
    async def _start(self) -> None:
        _LOGGER.info("Starting service...")

        async def coroutine() -> None:
            for i in range(5):
                _LOGGER.info("Coroutine running %d...", i)
                await sleep(0.1 + 0.4 * SYSTEM_RANDOM.random())
            _LOGGER.info("Raising...")
            raise CancelledError

        await coroutine()

    @override
    async def _stop(self) -> None:
        _LOGGER.info("Stopping service...")


def main() -> None:
    basic_config()
    _LOGGER.info("Running script...")
    run(_main())
    _LOGGER.info("Finished script")


async def _main() -> None:
    with suppress(CancelledError):
        async with Service() as service:
            _ = await service
