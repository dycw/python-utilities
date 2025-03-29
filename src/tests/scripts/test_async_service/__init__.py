from __future__ import annotations

from asyncio import CancelledError, Event, run, sleep
from contextlib import suppress
from logging import getLogger
from typing import override

from utilities.asyncio import AsyncLoopingService
from utilities.logging import basic_config

_LOGGER = getLogger(__name__)


class Parent(AsyncLoopingService):
    @override
    async def _run(self) -> None:
        _LOGGER.info("Running parent...")
        await sleep(1)


def main() -> None:
    basic_config()
    _LOGGER.info("Running script...")
    run(_main())


async def _main() -> None:
    await Parent().start()
    with suppress(CancelledError):
        _ = await Event().wait()
