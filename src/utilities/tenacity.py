from __future__ import annotations

from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any

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
from tenacity._utils import MAX_WAIT
from tenacity.asyncio import _portable_async_sleep
from typing_extensions import override

from utilities.contextlib import NoOpContextManager
from utilities.datetime import duration_to_float
from utilities.sentinel import Sentinel, sentinel

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Callable

    from tenacity.retry import RetryBaseT as SyncRetryBaseT
    from tenacity.stop import StopBaseT
    from tenacity.wait import WaitBaseT

    from utilities.asyncio import MaybeAwaitable
    from utilities.types import Duration


class wait_exponential_jitter(_wait_exponential_jitter):  # noqa: N801
    """Subclass of `wait_exponential_jitter` accepting durations."""

    @override
    def __init__(
        self,
        initial: Duration = 1,
        max: Duration = MAX_WAIT,
        exp_base: float = 2,
        jitter: Duration = 1,
    ) -> None:
        super().__init__(
            initial=duration_to_float(initial),
            max=duration_to_float(max),
            exp_base=exp_base,
            jitter=duration_to_float(jitter),
        )


@asynccontextmanager
async def yield_attempts(
    *,
    sleep: Callable[[int | float], MaybeAwaitable[None]] | Sentinel = sentinel,
    stop: StopBaseT | Sentinel = sentinel,
    wait: WaitBaseT | Sentinel = sentinel,
    retry: SyncRetryBaseT | Sentinel = sentinel,
    before: Callable[[RetryCallState], MaybeAwaitable[None]] | Sentinel = sentinel,
    after: Callable[[RetryCallState], MaybeAwaitable[None]] | Sentinel = sentinel,
    before_sleep: Callable[[RetryCallState], MaybeAwaitable[None]]
    | None
    | Sentinel = sentinel,
    reraise: bool | Sentinel = sentinel,
    retry_error_cls: type[RetryError] | Sentinel = sentinel,
    retry_error_callback: Callable[[RetryCallState], MaybeAwaitable[Any]]
    | None
    | Sentinel = sentinel,
) -> AsyncIterator[AttemptManager | NoOpContextManager]:
    """Yield the attempts."""
    args = (
        sleep,
        stop,
        wait,
        retry,
        before,
        after,
        before_sleep,
        reraise,
        retry_error_cls,
        retry_error_callback,
    )
    if all(isinstance(arg, Sentinel) for arg in args):
        yield NoOpContextManager()
    else:
        retrying = AsyncRetrying(
            sleep=_portable_async_sleep if isinstance(sleep, Sentinel) else sleep,
            stop=stop_never if isinstance(stop, Sentinel) else stop,
            wait=wait_none() if isinstance(wait, Sentinel) else wait,
            retry=retry_if_exception_type() if isinstance(retry, Sentinel) else retry,
            before=before_nothing if isinstance(before, Sentinel) else before,
            after=after_nothing if isinstance(after, Sentinel) else after,
            before_sleep=None if isinstance(before_sleep, Sentinel) else before_sleep,
            reraise=False if isinstance(reraise, Sentinel) else reraise,
            retry_error_cls=RetryError
            if isinstance(retry_error_cls, Sentinel)
            else retry_error_cls,
            retry_error_callback=None
            if isinstance(retry_error_callback, Sentinel)
            else retry_error_callback,
        )
        async for attempt in retrying:
            yield attempt


__all__ = ["wait_exponential_jitter", "yield_attempts"]
