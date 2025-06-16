from __future__ import annotations

from dataclasses import dataclass
from functools import wraps
from itertools import chain
from typing import TYPE_CHECKING, Any, ParamSpec, Self, TypeVar, cast, override

from arq.constants import default_queue_name, expires_extra_ms
from arq.cron import cron

from utilities.dataclasses import replace_non_sentinel
from utilities.sentinel import Sentinel, sentinel

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Sequence
    from datetime import datetime, timedelta, timezone

    from arq.connections import ArqRedis, RedisSettings
    from arq.cron import CronJob
    from arq.jobs import Deserializer, Job, Serializer
    from arq.typing import (
        OptionType,
        SecondsTimedelta,
        StartupShutdown,
        WeekdayOptionType,
        WorkerCoroutine,
    )
    from arq.worker import Function

    from utilities.types import CallableCoroutine1, Coroutine1, StrMapping


_P = ParamSpec("_P")
_T = TypeVar("_T")


##


def cron_raw(
    coroutine: CallableCoroutine1[Any],
    /,
    *,
    name: str | None = None,
    month: OptionType = None,
    day: OptionType = None,
    weekday: WeekdayOptionType = None,
    hour: OptionType = None,
    minute: OptionType = None,
    second: OptionType = 0,
    microsecond: int = 123_456,
    run_at_startup: bool = False,
    unique: bool = True,
    job_id: str | None = None,
    timeout: SecondsTimedelta | None = None,
    keep_result: float | None = 0,
    keep_result_forever: bool | None = False,
    max_tries: int | None = 1,
    args: Iterable[Any] | None = None,
    kwargs: StrMapping | None = None,
) -> CronJob:
    """Create a cron job with a raw coroutine function."""
    lifted = _lift_cron(
        coroutine, *(() if args is None else args), **({} if kwargs is None else kwargs)
    )
    return cron(
        lifted,
        name=name,
        month=month,
        day=day,
        weekday=weekday,
        hour=hour,
        minute=minute,
        second=second,
        microsecond=microsecond,
        run_at_startup=run_at_startup,
        unique=unique,
        job_id=job_id,
        timeout=timeout,
        keep_result=keep_result,
        keep_result_forever=keep_result_forever,
        max_tries=max_tries,
    )


def _lift_cron(
    func: Callable[_P, Coroutine1[_T]], *args: _P.args, **kwargs: _P.kwargs
) -> WorkerCoroutine:
    """Lift a coroutine function & call arg/kwargs for `cron`."""

    @wraps(func)
    async def wrapped(ctx: StrMapping, /) -> _T:
        _ = ctx
        return await func(*args, **kwargs)

    return cast("Any", wrapped)


##


@dataclass(kw_only=True, slots=True)
class _JobEnqueuer:
    """Enqueuer of jobs."""

    job_id: str | None = None
    queue_name: str | None = None
    defer_until: datetime | None = None
    defer_by: int | float | timedelta | None = None
    expires: int | float | timedelta | None = None
    job_try: int | None = None

    async def __call__(
        self,
        redis: ArqRedis,
        function: Callable[_P, Coroutine1[_T]],
        *args: _P.args,
        **kwargs: _P.kwargs,
    ) -> Job | None:
        return await redis.enqueue_job(  # skipif-ci-and-not-linux
            function.__name__,
            *args,
            _job_id=self.job_id,
            _queue_name=self.queue_name,
            _defer_until=self.defer_until,
            _defer_by=self.defer_by,
            _expires=self.expires,
            _job_try=self.job_try,
            **kwargs,
        )

    def settings(
        self,
        *,
        job_id: str | None | Sentinel = sentinel,
        queue_name: str | None | Sentinel = sentinel,
        defer_until: datetime | None | Sentinel = sentinel,
        defer_by: float | timedelta | None | Sentinel = sentinel,
        expires: float | timedelta | None | Sentinel = sentinel,
        job_try: int | None | Sentinel = sentinel,
    ) -> Self:
        """Replace elements of the enqueuer."""
        return replace_non_sentinel(  # skipif-ci-and-not-linux
            self,
            job_id=job_id,
            queue_name=queue_name,
            defer_until=defer_until,
            defer_by=defer_by,
            expires=expires,
            job_try=job_try,
        )


job_enqueuer = _JobEnqueuer()


##


class _WorkerMeta(type):
    @override
    def __new__(
        mcs: type[_WorkerMeta],
        name: str,
        bases: tuple[type, ...],
        namespace: dict[str, Any],
        /,
    ) -> type[Worker]:
        cls = cast("type[Worker]", super().__new__(mcs, name, bases, namespace))
        cls.functions = tuple(chain(cls.functions, map(cls._lift, cls.functions_raw)))
        return cls

    @classmethod
    def _lift(cls, func: Callable[_P, Coroutine1[_T]]) -> WorkerCoroutine:
        """Lift a coroutine function to accept the required `ctx` argument."""

        @wraps(func)
        async def wrapped(ctx: StrMapping, *args: _P.args, **kwargs: _P.kwargs) -> _T:
            _ = ctx
            return await func(*args, **kwargs)

        return cast("Any", wrapped)


@dataclass(kw_only=True)
class Worker(metaclass=_WorkerMeta):
    """Base class for all workers."""

    functions: Sequence[Function | WorkerCoroutine] = ()
    functions_raw: Sequence[CallableCoroutine1[Any]] = ()
    queue_name: str | None = default_queue_name
    cron_jobs: Sequence[CronJob] | None = None
    redis_settings: RedisSettings | None = None
    redis_pool: ArqRedis | None = None
    burst: bool = False
    on_startup: StartupShutdown | None = None
    on_shutdown: StartupShutdown | None = None
    on_job_start: StartupShutdown | None = None
    on_job_end: StartupShutdown | None = None
    after_job_end: StartupShutdown | None = None
    handle_signals: bool = True
    job_completion_wait: int = 0
    max_jobs: int = 10
    job_timeout: SecondsTimedelta = 300
    keep_result: SecondsTimedelta = 3600
    keep_result_forever: bool = False
    poll_delay: SecondsTimedelta = 0.5
    queue_read_limit: int | None = None
    max_tries: int = 5
    health_check_interval: SecondsTimedelta = 3600
    health_check_key: str | None = None
    ctx: dict[Any, Any] | None = None
    retry_jobs: bool = True
    allow_abort_jobs: bool = False
    max_burst_jobs: int = -1
    job_serializer: Serializer | None = None
    job_deserializer: Deserializer | None = None
    expires_extra_ms: int = expires_extra_ms
    timezone: timezone | None = None
    log_results: bool = True


__all__ = ["Worker", "cron", "job_enqueuer"]
