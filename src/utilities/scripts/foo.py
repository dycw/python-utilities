from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from logging import getLogger
from typing import TYPE_CHECKING, Any, override

from redis.asyncio import Redis

from utilities.asyncio import InfiniteLooper, InfiniteQueueLooper, sleep_dur
from utilities.datetime import MILLISECOND, SECOND
from utilities.logging import setup_logging
from utilities.orjson import deserialize
from utilities.redis import subscribe

if TYPE_CHECKING:
    from collections.abc import Iterator

    from utilities.types import Coroutine1, Duration, MaybeType

external: int = 0
logger = getLogger()
setup_logging()


async def increment_externally(obj: Example, /) -> None:
    global external
    while True:
        logger.info(f"incrementing externally {external=}, {obj.counter=}")
        external += 1
        obj.counter += 1
        logger.info(f"incrementing externally {external=}, {obj.counter=} after +1")
        await sleep_dur(duration=obj.sleep_core)


class CustomError(Exception): ...


@dataclass(kw_only=True)
class Example(InfiniteLooper[None]):
    sleep_core: Duration = 0.25
    sleep_restart: Duration = 5.0
    initializations: int = 0
    counter: int = 0

    @override
    async def _initialize(self) -> None:
        logger.info(f"initializing... {self=}")
        self.initializations += 1
        self.counter = 0
        logger.info(f"finished initializing {self=}")

    @override
    async def _core(self) -> None:
        logger.info(f"running core {self.counter=}")
        self.counter += 1
        logger.info(f"running core {self.counter=} after +1")
        if self.counter >= 10:
            logger.info(f"running core {self.counter=}; setting event")
            self._set_event(None)

    @override
    def _yield_coroutines(self) -> Iterator[Coroutine1[None]]:
        yield self.increment_externally()

    @override
    def _yield_events_and_exceptions(
        self,
    ) -> Iterator[tuple[None, MaybeType[BaseException]]]:
        yield (None, CustomError)

    @override
    def _error_upon_core(self, error: Exception, /) -> None:
        """Handle any errors upon running the core function."""
        if isinstance(error, ExceptionGroup):
            for i, error_i in enumerate(error.exceptions, start=1):
                logger.warning(f"Error #{i}: {error_i!r}")
        else:
            logger.warning(f"Non-group error: {error!r}")

    async def increment_externally(self) -> None:
        global external
        while True:
            logger.info(f"incrementing externally {external=}, {self.counter=}")
            external += 1
            self.counter += 1
            logger.info(
                f"incrementing externally {external=}, {self.counter=} after +1"
            )
            await sleep_dur(duration=self.sleep_core)


##################################################################
##################################################################
##################################################################
##################################################################
##################################################################
import traceback


class DebuggableCoro:
    def __init__(self, coro) -> None:
        self._coro = coro
        self._created_at = "".join(traceback.format_stack())
        self._used = False

    def __await__(self):
        if self._used:
            msg = f"Coroutine reused:\n{self._created_at}"
            raise RuntimeError(msg)
        self._used = True
        return self._coro.__await__()


@dataclass(kw_only=True)
class QueueExample(InfiniteQueueLooper[None, dict[str, Any]]):
    """Merge set of real time bars across data groups."""

    sleep_core: Duration = 1000 * MILLISECOND
    sleep_restart: Duration = 5 * SECOND
    subscribe_sleep: Duration = MILLISECOND
    subscribe_timeout: Duration = SECOND

    @override
    def __post_init__(self) -> None:
        super().__post_init__()
        self._redis = Redis()
        self._pubsub = self._redis.pubsub()

    @override
    async def _process_items(self, *bars: Any) -> None:
        logger.info(
            f"Processing {len(bars)=} items; {self._queue.qsize()=}, {len(bars)=}"
        )
        for bar in bars:
            self._process_item(bar)

    def _process_item(self, bar: Any, /) -> None: ...

    async def _subscribe_and_push(self) -> None:
        async for msg in subscribe(
            self._pubsub,
            "data-raw-5s",
            deserializer=deserialize,
            timeout=self.subscribe_timeout,
            sleep=self.subscribe_sleep,
        ):
            self.put_items_nowait(msg)

    @override
    def _yield_coroutines(self) -> Iterator[Coroutine1[None]]:
        yield self._subscribe_and_push()

    @override
    def _yield_events_and_exceptions(
        self,
    ) -> Iterator[tuple[None, MaybeType[BaseException]]]:
        yield (None, RuntimeError)

    @override
    def _error_upon_core(self, error: Exception, /) -> None:
        """Handle any errors upon running the core function."""
        if isinstance(error, ExceptionGroup):
            for i, error_i in enumerate(error.exceptions, start=1):
                logger.warning(f"Error #{i}: {error_i!r}")
        else:
            logger.warning(f"Non-group error: {error!r}")


##################################################################
##################################################################
##################################################################
##################################################################
##################################################################
##################################################################


def main() -> None:
    asyncio.run(_main())


async def _main() -> None:
    # await Example()()
    await QueueExample()()


if __name__ == "__main__":
    main()
