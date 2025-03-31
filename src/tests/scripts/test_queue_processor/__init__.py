from __future__ import annotations

from asyncio import CancelledError, run, sleep
from contextlib import suppress
from logging import getLogger
from typing import override

from utilities.asyncio import QueueProcessor
from utilities.functions import get_class_name
from utilities.logging import basic_config
from utilities.random import SYSTEM_RANDOM, bernoulli

_LOGGER = getLogger(__name__)


class Processor(QueueProcessor[int]):
    @override
    async def _process_item(self, _: int, /) -> None:
        _LOGGER.info("Processing:   -> %d", len(self))
        if bernoulli(true=0.25):
            msg = "Encountered a random failure!"
            raise ValueError(msg)
        await sleep(1)
        if self.empty():
            raise CancelledError

    @override
    async def _process_item_failure(self, item: int, error: Exception, /) -> None:
        _LOGGER.error("%s - %s", item, get_class_name(error))


def main() -> None:
    basic_config()
    _LOGGER.info("Running script...")
    with suppress(CancelledError):
        run(_main())
    _LOGGER.info("Finished script")


async def populate(processor: Processor, /) -> None:
    while len(processor) <= 9:
        init = len(processor)
        processor.enqueue(SYSTEM_RANDOM.randint(0, 9))
        post = len(processor)
        _LOGGER.info("Populating: %d -> %d", init, post)
        await sleep(0.1 + 0.4 * SYSTEM_RANDOM.random())


async def _main() -> None:
    async with Processor() as processor:
        await populate(processor)
