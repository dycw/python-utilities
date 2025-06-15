from __future__ import annotations

from dataclasses import dataclass
from functools import wraps
from typing import TYPE_CHECKING, Any, Concatenate, ParamSpec, TypeVar, reveal_type

from arq.constants import default_queue_name, expires_extra_ms

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence
    from datetime import timezone

    from arq.connections import ArqRedis, RedisSettings
    from arq.cron import CronJob
    from arq.jobs import Deserializer, Serializer
    from arq.typing import SecondsTimedelta, StartupShutdown, WorkerCoroutine
    from arq.worker import Function

    from utilities.types import Coroutine1, StrMapping

_P = ParamSpec("_P")
_T = TypeVar("_T")


def lift(
    func: Callable[_P, Coroutine1[_T]],
) -> Callable[Concatenate[StrMapping, _P], Coroutine1[_T]]:
    """Lift a coroutine function to accept the required `ctx` argument."""

    @wraps(func)
    async def wrapped(ctx: dict[str, Any], *args: Any, **kwargs: Any) -> Any:
        _ = ctx
        return await func(*args, **kwargs)

    return wrapped


@lift
async def foo(x: int) -> float: ...


reveal_type(foo)

##


@dataclass(kw_only=True)
class Worker:
    """Base class for all workers."""

    functions: Sequence[Function | WorkerCoroutine] = ()
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


__all__ = ["Worker"]
