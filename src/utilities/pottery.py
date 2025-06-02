from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager, suppress
from typing import TYPE_CHECKING

from pottery import AIORedlock
from pottery.exceptions import ReleaseUnlockedLock
from redis.asyncio import Redis

from utilities.datetime import MILLISECOND, SECOND, datetime_duration_to_float
from utilities.iterables import always_iterable

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from utilities.types import Duration, MaybeIterable


@asynccontextmanager
async def yield_locked_resource(
    redis: MaybeIterable[Redis],
    key: str,
    /,
    *,
    duration: Duration = 10 * SECOND,
    sleep: Duration = MILLISECOND,
) -> AsyncIterator[None]:
    """Yield a locked resource."""
    masters = (  # skipif-ci-and-not-linux
        {redis} if isinstance(redis, Redis) else set(always_iterable(redis))
    )
    duration_use = datetime_duration_to_float(duration)  # skipif-ci-and-not-linux
    lock = AIORedlock(  # skipif-ci-and-not-linux
        key=key,
        masters=masters,
        auto_release_time=duration_use,
        context_manager_timeout=duration_use,
    )
    sleep_use = datetime_duration_to_float(sleep)  # skipif-ci-and-not-linux
    while not await lock.acquire():  # pragma: no cover
        _ = await asyncio.sleep(sleep_use)
    try:  # skipif-ci-and-not-linux
        yield
    finally:  # skipif-ci-and-not-linux
        with suppress(ReleaseUnlockedLock):
            await lock.release()


__all__ = ["yield_locked_resource"]
