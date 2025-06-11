from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager, suppress
from dataclasses import dataclass
from typing import TYPE_CHECKING, override

from pottery import AIORedlock
from pottery.exceptions import ReleaseUnlockedLock
from redis.asyncio import Redis

from utilities.asyncio import sleep_dur, timeout_dur
from utilities.datetime import MILLISECOND, SECOND, datetime_duration_to_float
from utilities.iterables import always_iterable

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterable

    from utilities.types import Duration, MaybeIterable


@asynccontextmanager
async def yield_locked_resource(
    redis: MaybeIterable[Redis],
    key: str,
    /,
    *,
    duration: Duration = 10 * SECOND,
    sleep: Duration = MILLISECOND,
) -> AsyncIterator[AIORedlock]:
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
        yield lock
    finally:  # skipif-ci-and-not-linux
        with suppress(ReleaseUnlockedLock):
            await lock.release()


@asynccontextmanager
async def yield_access(
    redis: MaybeIterable[Redis],
    key: str,
    /,
    *,
    num: int = 1,
    timeout_acquire: Duration | None = None,
    timeout_release: Duration = 10 * SECOND,
    sleep_wait: Duration = MILLISECOND,
    sleep_post_release: Duration | None = None,
) -> AsyncIterator[None]:
    """Acquire access to a locked resource, amongst 1 of multiple connections."""
    if num <= 0:
        raise _YieldAccessNumLocksError(key=key, num=num)
    masters = (  # skipif-ci-and-not-linux
        {redis} if isinstance(redis, Redis) else set(always_iterable(redis))
    )
    auto_release_time = datetime_duration_to_float(  # skipif-ci-and-not-linux
        timeout_release
    )
    locks = [  # skipif-ci-and-not-linux
        AIORedlock(
            key=f"{key}_{i}_of_{num}",
            masters=masters,
            auto_release_time=auto_release_time,
        )
        for i in range(1, num + 1)
    ]
    lock: AIORedlock | None = None  # skipif-ci-and-not-linux
    try:  # skipif-ci-and-not-linux
        lock = await _get_first_available_lock(
            key, locks, num=num, timeout_acquire=timeout_acquire, sleep_wait=sleep_wait
        )
        yield
    finally:  # skipif-ci-and-not-linux
        await sleep_dur(duration=sleep_post_release)
        if lock is not None:
            with suppress(ReleaseUnlockedLock):
                await lock.release()


async def _get_first_available_lock(
    key: str,
    locks: Iterable[AIORedlock],
    /,
    *,
    num: int = 1,
    timeout_acquire: Duration | None = None,
    sleep_wait: Duration | None = None,
) -> AIORedlock:
    locks = list(locks)  # skipif-ci-and-not-linux
    error = _YieldAccessUnableToAcquireLockError(  # skipif-ci-and-not-linux
        key=key, num=num, timeout=timeout_acquire
    )
    async with timeout_dur(  # skipif-ci-and-not-linux
        duration=timeout_acquire, error=error
    ):
        while True:
            if (result := await _get_first_available_lock_if_any(locks)) is not None:
                return result
            await sleep_dur(duration=sleep_wait)


async def _get_first_available_lock_if_any(
    locks: Iterable[AIORedlock], /
) -> AIORedlock | None:
    for lock in locks:  # skipif-ci-and-not-linux
        if await lock.acquire(blocking=False):
            return lock
    return None  # skipif-ci-and-not-linux


@dataclass(kw_only=True, slots=True)
class YieldAccessError(Exception):
    key: str
    num: int


@dataclass(kw_only=True, slots=True)
class _YieldAccessNumLocksError(YieldAccessError):
    @override
    def __str__(self) -> str:
        return f"Number of locks for {self.key!r} must be positive; got {self.num}"


@dataclass(kw_only=True, slots=True)
class _YieldAccessUnableToAcquireLockError(YieldAccessError):
    timeout: Duration | None

    @override
    def __str__(self) -> str:
        return f"Unable to acquire any 1 of {self.num} locks for {self.key!r} after {self.timeout}"  # skipif-ci-and-not-linux


__all__ = ["YieldAccessError", "yield_access", "yield_locked_resource"]
