from __future__ import annotations

from contextlib import AbstractAsyncContextManager, asynccontextmanager
from typing import TYPE_CHECKING, Any, override

from tenacity import (
    AsyncRetrying,
    AttemptManager,
    RetryCallState,
    RetryError,
    after_nothing,
    before_nothing,
    retry_if_exception_type,
    stop_never,
    wait_none,
)
from tenacity import wait_exponential_jitter as _wait_exponential_jitter
from tenacity.asyncio import _portable_async_sleep

from utilities.asyncio import timeout_delta
from utilities.contextlib import NoOpContextManager
from utilities.whenever import HOUR, SECOND

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Callable

    from tenacity.retry import RetryBaseT
    from tenacity.stop import StopBaseT
    from tenacity.wait import WaitBaseT
    from whenever import TimeDelta

    from utilities.types import MaybeAwaitable


type MaybeAttemptManager = NoOpContextManager | AttemptManager
type MaybeAttemptContextManager = AbstractAsyncContextManager[MaybeAttemptManager]


class wait_exponential_jitter(_wait_exponential_jitter):  # noqa: N801
    """Subclass of `wait_exponential_jitter` accepting TimeDeltas."""

    @override
    def __init__(
        self,
        initial: TimeDelta = SECOND,
        max: TimeDelta = HOUR,
        exp_base: TimeDelta = 2 * SECOND,
        jitter: TimeDelta = SECOND,
    ) -> None:
        super().__init__(
            initial=initial.in_seconds(),
            max=max.in_seconds(),
            exp_base=exp_base.in_seconds(),
            jitter=jitter.in_seconds(),
        )


async def yield_attempts(
    *,
    sleep: Callable[[int | float], MaybeAwaitable[None]] | None = None,
    stop: StopBaseT | None = None,
    wait: WaitBaseT | None = None,
    retry: RetryBaseT | None = None,
    before: Callable[[RetryCallState], MaybeAwaitable[None]] | None = None,
    after: Callable[[RetryCallState], MaybeAwaitable[None]] | None = None,
    before_sleep: Callable[[RetryCallState], MaybeAwaitable[None]] | None = None,
    reraise: bool | None = None,
    retry_error_cls: type[RetryError] | None = None,
    retry_error_callback: Callable[[RetryCallState], MaybeAwaitable[Any]] | None = None,
) -> AsyncIterator[MaybeAttemptManager]:
    """Yield the attempts."""
    if (
        (sleep is None)
        and (stop is None)
        and (wait is None)
        and (retry is None)
        and (before is None)
        and (after is None)
        and (before_sleep is None)
        and (reraise is None)
        and (retry_error_cls is None)
    ):
        yield NoOpContextManager()
    else:
        retrying = AsyncRetrying(
            sleep=_portable_async_sleep if sleep is None else sleep,
            stop=stop_never if stop is None else stop,
            wait=wait_none() if wait is None else wait,
            retry=retry_if_exception_type() if retry is None else retry,
            before=before_nothing if before is None else before,
            after=after_nothing if after is None else after,
            before_sleep=None if before_sleep is None else before_sleep,
            reraise=False if reraise is None else reraise,
            retry_error_cls=RetryError if retry_error_cls is None else retry_error_cls,
            retry_error_callback=retry_error_callback,
        )
        async for attempt in retrying:
            yield attempt


async def yield_timeout_attempts(
    *,
    sleep: Callable[[int | float], MaybeAwaitable[None]] | None = None,
    stop: StopBaseT | None = None,
    wait: WaitBaseT | None = None,
    retry: RetryBaseT | None = None,
    before: Callable[[RetryCallState], MaybeAwaitable[None]] | None = None,
    after: Callable[[RetryCallState], MaybeAwaitable[None]] | None = None,
    before_sleep: Callable[[RetryCallState], MaybeAwaitable[None]] | None = None,
    reraise: bool | None = None,
    retry_error_cls: type[RetryError] | None = None,
    retry_error_callback: Callable[[RetryCallState], MaybeAwaitable[Any]] | None = None,
    delta: TimeDelta | None = None,
) -> AsyncIterator[MaybeAttemptContextManager]:
    """Yield the attempts, with timeout."""
    async for attempt in yield_attempts(
        sleep=sleep,
        stop=stop,
        wait=wait,
        retry=retry,
        before=before,
        after=after,
        before_sleep=before_sleep,
        reraise=reraise,
        retry_error_cls=retry_error_cls,
        retry_error_callback=retry_error_callback,
    ):

        @asynccontextmanager
        async def new(
            attempt: MaybeAttemptManager, /
        ) -> AsyncIterator[MaybeAttemptManager]:
            with attempt:
                async with timeout_delta(delta):
                    yield attempt

        yield new(attempt)


__all__ = [
    "MaybeAttemptManager",
    "wait_exponential_jitter",
    "yield_attempts",
    "yield_timeout_attempts",
]
