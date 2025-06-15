from __future__ import annotations

from dataclasses import dataclass, replace
from functools import wraps
from itertools import chain
from typing import TYPE_CHECKING, Any, ParamSpec, TypeVar, cast

from arq.constants import default_queue_name, expires_extra_ms
from arq.cron import cron

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence
    from datetime import timezone

    from arq.connections import ArqRedis, RedisSettings
    from arq.cron import CronJob
    from arq.jobs import Deserializer, Serializer
    from arq.typing import (
        OptionType,
        SecondsTimedelta,
        StartupShutdown,
        WeekdayOptionType,
        WorkerCoroutine,
    )
    from arq.worker import Function

    from utilities.types import CallableAwaitable, Coroutine1


def cron_raw(
    coroutine: CallableAwaitable[Any],
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
) -> CronJob:
    """Create a cron job with a raw coroutine function."""
    return cron(
        lift(coroutine),
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


##


_P = ParamSpec("_P")
_T = TypeVar("_T")


def lift(func: Callable[_P, Coroutine1[_T]]) -> WorkerCoroutine:
    """Lift a coroutine function to accept the required `ctx` argument."""

    @wraps(func)
    async def wrapped(ctx: dict[Any, Any], *args: _P.args, **kwargs: _P.kwargs) -> _T:
        _ = ctx
        return await func(*args, **kwargs)

    return cast("Any", wrapped)


##


class _WorkerMeta(type):
    def __new__(
        mcs: type[_WorkerMeta],
        name: str,
        bases: tuple[type, ...],
        namespace: dict[str, Any],
        /,
    ) -> type[Worker]:
        cls = cast("type[Worker]", super().__new__(mcs, name, bases, namespace))
        cls.functions = tuple(chain(cls.functions, map(lift, cls.functions_raw)))
        if cls.cron_jobs_raw is not None:
            lifted = (
                replace(c, coroutine=lift(c.coroutine)) for c in cls.cron_jobs_raw
            )
            cls.cron_jobs = tuple(
                chain([] if cls.cron_jobs is None else list(cls.cron_jobs), lifted)
            )
        return cls


@dataclass(kw_only=True)
class Worker(metaclass=_WorkerMeta):
    """Base class for all workers."""

    functions: Sequence[Function | WorkerCoroutine] = ()
    functions_raw: Sequence[CallableAwaitable[Any]] = ()
    queue_name: str | None = default_queue_name
    cron_jobs: Sequence[CronJob] | None = None
    cron_jobs_raw: Sequence[CronJob] | None = None
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


__all__ = ["Worker", "lift"]
