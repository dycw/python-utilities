from __future__ import annotations

from asyncio import CancelledError, run, sleep
from contextlib import suppress
from logging import getLogger
from typing import override

from utilities.asyncio import AsyncService
from utilities.logging import basic_config
from utilities.random import SYSTEM_RANDOM

_LOGGER = getLogger(__name__)


class Service(AsyncService):
    @override
    async def _start(self) -> None:
        _LOGGER.info("Starting service...")

        async def coroutine() -> None:
            for i in range(1, 6):
                _LOGGER.info("Run #%d...", i)
                await sleep(0.1 + 0.4 * SYSTEM_RANDOM.random())
            _LOGGER.info("Cancelling...")
            raise CancelledError

        await coroutine()

    @override
    async def stop(self) -> None:
        _LOGGER.info("Stopping service...")
        await super().stop()


def main() -> None:
    basic_config()
    _LOGGER.info("Running script...")
    with suppress(CancelledError):
        run(_main())
    _LOGGER.info("Finished script")


async def _main() -> None:
    async with Service():
        ...
