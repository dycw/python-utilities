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
    async def _process_item(self, item: int, /) -> None:
        _LOGGER.info("Processing %d; %d left...", item, len(self))
        if bernoulli(true=0.33):
            msg = "Encountered a random failure!"
            raise ValueError(msg)
        await sleep(1)
        if len(self) == 0:
            _LOGGER.info("Stopping...")
            await self._stop()


def main() -> None:
    basic_config()
    _LOGGER.info("Running script...")
    run(_main())
    _LOGGER.info("Finished script")


async def populate(processor: Processor, /) -> None:
    while len(processor) <= 10:
        init = len(processor)
        processor.enqueue(SYSTEM_RANDOM.randint(0, 9))
        post = len(processor)
        _LOGGER.info("%d -> %d items", init, post)
        await sleep(0.1 + 0.4 * SYSTEM_RANDOM.random())


async def _main() -> None:
    def callback(item: int, error: Exception, /) -> None:
        _LOGGER.error("Failed to process %d because of %s", item, get_class_name(error))

    processor = Processor(process_item_failure=callback)
    await processor._start()
    with suppress(CancelledError):
        await populate(processor)
