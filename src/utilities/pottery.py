from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager, suppress
from typing import TYPE_CHECKING

from pottery import AIORedlock
from pottery.exceptions import ReleaseUnlockedLock

from utilities.datetime import MILLISECOND, SECOND, datetime_duration_to_float

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from redis.asyncio import Redis

    from utilities.types import Duration


@asynccontextmanager
async def yield_aio_redlock(
    redis: Redis,
    key: str,
    /,
    *,
    duration: Duration = 10 * SECOND,
    sleep: Duration = MILLISECOND,
) -> AsyncIterator[None]:
    duration_use = datetime_duration_to_float(duration)
    sleep_use = datetime_duration_to_float(sleep)
    while True:
        lock = AIORedlock(
            key=key,
            masters={redis},
            auto_release_time=duration_use,
            context_manager_timeout=duration_use,
        )
        if await lock.acquire():
            break
        await asyncio.sleep(sleep_use)
    try:
        yield
    finally:
        with suppress(ReleaseUnlockedLock):
            await lock.release()
