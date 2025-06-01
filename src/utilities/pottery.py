from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager, suppress
from typing import TYPE_CHECKING

from pottery import AIORedlock
from pottery.exceptions import ReleaseUnlockedLock

from utilities.datetime import MILLISECOND, SECOND, datetime_duration_to_float
from utilities.iterables import always_iterable

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from redis.asyncio import Redis

    from utilities.types import Duration, MaybeIterable


@asynccontextmanager
async def yield_locked_resource(
    redis_or_redises: MaybeIterable[Redis],
    key: str,
    /,
    *,
    duration: Duration = 10 * SECOND,
    sleep: Duration = MILLISECOND,
) -> AsyncIterator[None]:
    """Yield a locked resource."""
    masters = set(always_iterable(redis_or_redises))  # skipif-ci-and-not-linux
    duration_use = datetime_duration_to_float(duration)  # skipif-ci-and-not-linux
    sleep_use = datetime_duration_to_float(sleep)  # skipif-ci-and-not-linux
    while True:  # skipif-ci-and-not-linux
        lock = AIORedlock(
            key=key,
            masters=masters,
            auto_release_time=duration_use,
            context_manager_timeout=duration_use,
        )
        if await lock.acquire():
            break
        await asyncio.sleep(sleep_use)
    try:  # skipif-ci-and-not-linux
        yield
    finally:  # skipif-ci-and-not-linux
        with suppress(ReleaseUnlockedLock):
            await lock.release()


__all__ = ["yield_locked_resource"]
