from __future__ import annotations

from asyncio import Event
from contextlib import asynccontextmanager, suppress
from typing import TYPE_CHECKING

from pottery import AIORedlock
from pottery.exceptions import ReleaseUnlockedLock
from redis.asyncio import Redis

from utilities.datetime import SECOND, datetime_duration_to_float
from utilities.iterables import always_iterable

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from utilities.types import Duration, MaybeIterable


@asynccontextmanager
async def yield_locked_resource(
    redis: MaybeIterable[Redis], key: str, /, *, duration: Duration = 10 * SECOND
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
    while not await lock.acquire():  # skipif-ci-and-not-linux
        _ = await Event().wait()
    try:  # skipif-ci-and-not-linux
        yield
    finally:  # skipif-ci-and-not-linux
        with suppress(ReleaseUnlockedLock):
            await lock.release()


__all__ = ["yield_locked_resource"]
