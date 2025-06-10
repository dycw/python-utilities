from __future__ import annotations

import asyncio
import time
from contextlib import asynccontextmanager, contextmanager, suppress
from typing import TYPE_CHECKING

import redis
import redis.asyncio
from pottery import AIORedlock, Redlock
from pottery.exceptions import ReleaseUnlockedLock

from utilities.datetime import MILLISECOND, SECOND, datetime_duration_to_float
from utilities.iterables import always_iterable

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterator

    from utilities.types import Duration, MaybeIterable


@contextmanager
def yield_locked_resource(
    redis_: MaybeIterable[redis.Redis],
    key: str,
    /,
    *,
    duration: Duration = 10 * SECOND,
    sleep: Duration = MILLISECOND,
) -> Iterator[None]:
    """Yield a locked resource."""
    masters = (  # skipif-ci-and-not-linux
        {redis_} if isinstance(redis_, redis.Redis) else set(always_iterable(redis_))
    )
    duration_use = datetime_duration_to_float(duration)  # skipif-ci-and-not-linux
    lock = Redlock(  # skipif-ci-and-not-linux
        key=key,
        masters=masters,
        auto_release_time=duration_use,
        context_manager_timeout=duration_use,
    )
    sleep_use = datetime_duration_to_float(sleep)  # skipif-ci-and-not-linux
    while not lock.acquire():  # pragma: no cover
        _ = time.sleep(sleep_use)
    try:  # skipif-ci-and-not-linux
        yield
    finally:  # skipif-ci-and-not-linux
        with suppress(ReleaseUnlockedLock):
            lock.release()


@asynccontextmanager
async def yield_async_locked_resource(
    redis_: MaybeIterable[redis.asyncio.Redis],
    key: str,
    /,
    *,
    duration: Duration = 10 * SECOND,
    sleep: Duration = MILLISECOND,
) -> AsyncIterator[None]:
    """Yield a locked resource."""
    masters = (  # skipif-ci-and-not-linux
        {redis_}
        if isinstance(redis_, redis.asyncio.Redis)
        else set(always_iterable(redis_))
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


__all__ = ["yield_async_locked_resource"]
