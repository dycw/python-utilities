from __future__ import annotations

from asyncio import CancelledError, Event, run, sleep
from contextlib import suppress
from logging import getLogger
from typing import override

from utilities.asyncio import QueueProcessor
from utilities.logging import basic_config
from utilities.random import SYSTEM_RANDOM

_LOGGER = getLogger(__name__)


class Processor(QueueProcessor[int]):
    @override
    async def _process_item(self, item: int, /) -> None:
        _LOGGER.info("Processing %d; %d left...", item, len(self))
        await sleep(1)


def main() -> None:
    basic_config()
    _LOGGER.info("Running script...")
    run(_main())


async def populate(processor: Processor, /) -> None:
    while len(processor) <= 10:
        init = len(processor)
        processor.enqueue(SYSTEM_RANDOM.randint(0, 9))
        post = len(processor)
        _LOGGER.info("%d -> %d items", init, post)
        await sleep(0.1 + 0.4 * SYSTEM_RANDOM.random())


async def _main() -> None:
    processor = Processor()
    with suppress(CancelledError):
        await processor.start()
        await populate(processor)
        _ = await Event().wait()
