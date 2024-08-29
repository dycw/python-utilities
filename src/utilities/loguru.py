from __future__ import annotations

import asyncio
import logging
import sys
import time
from logging import Handler, LogRecord
from sys import __excepthook__, _getframe
from typing import TYPE_CHECKING

from loguru import logger
from typing_extensions import override

from utilities.datetime import duration_to_timedelta
from utilities.logging import LogLevel
from utilities.reprlib import custom_repr

if TYPE_CHECKING:
    from types import TracebackType

    from loguru import Record

    from utilities.types import Duration


class InterceptHandler(Handler):
    """Handler for intercepting standard logging messages.

    https://github.com/Delgan/loguru#entirely-compatible-with-standard-logging
    """

    @override
    def emit(self, record: LogRecord) -> None:
        # Get corresponding Loguru level if it exists.
        try:  # pragma: no cover
            level = logger.level(record.levelname).name
        except ValueError:  # pragma: no cover
            level = record.levelno

        # Find caller from where originated the logged message.
        frame, depth = _getframe(6), 6  # pragma: no cover
        while (  # pragma: no cover
            frame and frame.f_code.co_filename == logging.__file__
        ):
            frame = frame.f_back
            depth += 1

        logger.opt(depth=depth, exception=record.exc_info).log(  # pragma: no cover
            level, record.getMessage()
        )


catch_message = "Uncaught {record[exception].value!r} ({record[process].name}/{record[process].id} | {record[thread].name}/{record[thread].id})"


def except_hook(
    exc_type: type[BaseException],
    exc_value: BaseException,
    exc_traceback: TracebackType | None,
    /,
) -> None:
    """Exception hook which uses `loguru`."""
    if issubclass(exc_type, KeyboardInterrupt):
        __excepthook__(exc_type, exc_value, exc_traceback)
        return
    logger.opt(exception=exc_value, record=True).error(catch_message)
    sys.exit(1)


def format_record(record: Record, /) -> str:
    """Format a record."""
    parts = [
        "<green>{time:YYYY-MM-DD HH:mm:ss}</green>{time:.SSS zz/ddd}",
        "<cyan>{name}</cyan>.<cyan>{function}</cyan>:{line}",
        "<level>{level}</level>",
    ]
    if record["message"]:
        parts.append("<level>{message}</level>")
    if "custom_repr" in record["extra"]:
        parts.append("{extra[custom_repr]}")
    fmt = " | ".join(parts) + "\n"
    if record["exception"] is not None:
        fmt += "{exception}\n"
    return fmt


def format_record_json(record: Record, /) -> str:
    """Format a record for JSON."""
    parts = []
    if "json" in record["extra"]:
        parts.append("{extra[json]}")
    return " | ".join(parts) + "\n"


def _log_call_bind_and_log(
    func: Callable[..., Any], level: LogLevel, /, *args: Any, **kwargs: Any
) -> None:
    func_name = get_func_name(func)
    key = f"<{func_name}>"
    bound_args = bind_args_custom_repr(func, *args, **kwargs)
    _log_from_depth_up(logger, 3, level, "", exception=None, **{key: bound_args})


def logged_sleep_sync(
    duration: Duration, /, *, level: LogLevel = LogLevel.INFO, depth: int = 1
) -> None:
    """Log a sleep operation, synchronously."""
    timedelta = duration_to_timedelta(duration)
    logger.opt(depth=depth).log(
        level, "Sleeping for {timedelta}...", timedelta=timedelta
    )
    time.sleep(timedelta.total_seconds())


async def logged_sleep_async(
    duration: Duration, /, *, level: LogLevel = LogLevel.INFO, depth: int = 1
) -> None:
    """Log a sleep operation, asynchronously."""
    timedelta = duration_to_timedelta(duration)
    logger.opt(depth=depth).log(
        level, "Sleeping for {timedelta}...", timedelta=timedelta
    )
    await asyncio.sleep(timedelta.total_seconds())


def _patch_custom_repr(record: Record, /) -> None:
    """Add the `custom_repr` field to the extras."""
    mapping = {
        k: v for k, v in record["extra"].items() if k not in {"json", "custom_repr"}
    }
    record["extra"]["custom_repr"] = custom_repr(mapping)


def _patch_json(record: Record, /) -> None:
    """Add the `json` field to the extras."""
    record["extra"]["json"] = _serialize_record(record)


patched_logger = logger.patch(_patch_custom_repr).patch(_patch_json)


def _serialize_record(record: Record, /) -> str:
    """Serialize a record."""
    from orjson import dumps

    use = {}
    use["time"] = record["time"]
    use["name"] = record["name"]
    use["module"] = record["module"]
    use["function"] = record["function"]
    use["line"] = record["line"]
    use["message"] = record["message"]
    use |= record["extra"]
    if record["exception"] is not None:
        use["exception"] = {"type": str(record["exception"])}
    return dumps(use, default=str).decode()


def make_filter(
    *,
    level: LogLevel | None = None,
    min_level: LogLevel | None = None,
    max_level: LogLevel | None = None,
    name_include: MaybeIterable[str] | None = None,
    name_exclude: MaybeIterable[str] | None = None,
    extra_include_all: MaybeIterable[Hashable] | None = None,
    extra_include_any: MaybeIterable[Hashable] | None = None,
    extra_exclude_all: MaybeIterable[Hashable] | None = None,
    extra_exclude_any: MaybeIterable[Hashable] | None = None,
    _is_testing_override: bool = False,
) -> FilterFunction:
    """Make a filter."""
    is_not_pytest_or_override = (not is_pytest()) or _is_testing_override

    def filter_func(record: Record, /) -> bool:
        rec_level_no = record["level"].no
        if (level is not None) and (rec_level_no != get_logging_level(level)):
            return False
        if (min_level is not None) and (rec_level_no < get_logging_level(min_level)):
            return False
        if (max_level is not None) and (rec_level_no > get_logging_level(max_level)):
            return False
        name = record["name"]
        if name is not None:
            name_inc, name_exc = resolve_include_and_exclude(
                include=name_include, exclude=name_exclude
            )
            if (name_inc is not None) and not any(name.startswith(n) for n in name_inc):
                return False
            if (name_exc is not None) and any(name.startswith(n) for n in name_exc):
                return False
        rec_extra_keys = set(record["extra"])
        extra_inc_all, extra_exc_any = resolve_include_and_exclude(
            include=extra_include_all, exclude=extra_exclude_any
        )
        if (extra_inc_all is not None) and not extra_inc_all.issubset(rec_extra_keys):
            return False
        if (extra_exc_any is not None) and (len(rec_extra_keys & extra_exc_any) >= 1):
            return False
        extra_inc_any, extra_exc_all = resolve_include_and_exclude(
            include=extra_include_any, exclude=extra_exclude_all
        )
        if (extra_inc_any is not None) and (len(rec_extra_keys & extra_inc_any) == 0):
            return False
        if (extra_exc_all is not None) and extra_exc_all.issubset(rec_extra_keys):
            return False
        return is_not_pytest_or_override

    return filter_func


def _log_from_depth_up(
    logger: Logger,
    depth: int,
    level: LogLevel,
    message: str,
    /,
    *args: Any,
    exception: bool | ExcInfo | BaseException | None = None,
    **kwargs: Any,
) -> None:
    """Log from a given depth up to 0, in case it would fail otherwise."""
    if depth >= 0:
        try:
            logger.opt(exception=exception, record=True, depth=depth).log(
                level, message, *args, **kwargs
            )
        except ValueError:  # pragma: no cover
            return _log_from_depth_up(
                logger, depth - 1, level, message, *args, exception=exception, **kwargs
            )
        return None
    raise _LogFromDepthUpError(depth=depth)


@dataclass(kw_only=True)
class _LogFromDepthUpError(Exception):
    depth: int

    @override
    def __str__(self) -> str:
        return f"Depth must be non-negative; got {self.depth}"


__all__ = [
    "InterceptHandler",
    "catch_message",
    "except_hook",
    "format_record",
    "format_record_json",
    "logged_sleep_async",
    "logged_sleep_sync",
    "patched_logger",
]
