from __future__ import annotations

from asyncio import TaskGroup, run
from collections.abc import Callable
from dataclasses import dataclass
from functools import partial
from inspect import iscoroutinefunction
from logging import getLogger
from pathlib import Path
from sys import version_info
from typing import TYPE_CHECKING, cast

from typing_extensions import override

from utilities.asyncio import Coroutine1
from utilities.atomicwrites import writer
from utilities.datetime import get_now
from utilities.logging import LoggerOrName, get_default_logging_path, get_logger
from utilities.traceback import assemble_exception_paths

if TYPE_CHECKING:
    from collections.abc import Iterable
    from types import TracebackType

    from utilities.asyncio import MaybeCoroutine1
    from utilities.types import PathLike, StrMapping

_LOGGER = getLogger(__name__)
VERSION_MAJOR_MINOR = (version_info.major, version_info.minor)


def _get_default_logging_path() -> Path:
    """Get the logging default path."""
    return get_default_logging_path().joinpath("errors")


def make_except_hook(
    *,
    logger: LoggerOrName = _LOGGER,
    log_raw: bool = False,
    log_raw_extra: StrMapping | None = None,
    max_width: int = 80,
    indent_size: int = 4,
    max_length: int | None = None,
    max_string: int | None = None,
    max_depth: int | None = None,
    expand_all: bool = False,
    log_assembled: bool = False,
    log_assembled_extra: StrMapping | None = None,
    log_assembled_dir: PathLike | Callable[[], Path] | None = _get_default_logging_path,
    callbacks: Iterable[Callable[[], MaybeCoroutine1[None]]] | None = None,
) -> Callable[
    [type[BaseException] | None, BaseException | None, TracebackType | None], None
]:
    """Create an exception hook with various features."""
    return partial(
        _make_except_hook_inner,
        logger=logger,
        log_raw=log_raw,
        log_raw_extra=log_raw_extra,
        max_width=max_width,
        indent_size=indent_size,
        max_length=max_length,
        max_string=max_string,
        max_depth=max_depth,
        expand_all=expand_all,
        log_assembled=log_assembled,
        log_assembled_extra=log_assembled_extra,
        log_assembled_dir=log_assembled_dir,
        callbacks=callbacks,
    )


def _make_except_hook_inner(
    exc_type: type[BaseException] | None,
    exc_val: BaseException | None,
    traceback: TracebackType | None,
    /,
    *,
    logger: LoggerOrName,
    log_raw: bool = False,
    log_raw_extra: StrMapping | None = None,
    max_width: int = 80,
    indent_size: int = 4,
    max_length: int | None = None,
    max_string: int | None = None,
    max_depth: int | None = None,
    expand_all: bool = False,
    log_assembled: bool = False,
    log_assembled_extra: StrMapping | None = None,
    log_assembled_dir: PathLike | Callable[[], Path] | None = _get_default_logging_path,
    callbacks: Iterable[Callable[[], MaybeCoroutine1[None]]] | None = None,
) -> None:
    """Exception hook to log the traceback."""
    _ = (exc_type, traceback)
    if exc_val is None:
        raise MakeExceptHookError
    logger = get_logger(logger)
    if log_raw:
        logger.error("%s", exc_val, extra=log_raw_extra)
    error = assemble_exception_paths(exc_val)
    try:
        from rich.pretty import pretty_repr
    except ImportError:  # pragma: no cover
        repr_use = repr(error)
    else:
        repr_use = pretty_repr(
            error,
            max_width=max_width,
            indent_size=indent_size,
            max_length=max_length,
            max_string=max_string,
            max_depth=max_depth,
            expand_all=expand_all,
        )
    if log_assembled:
        logger.error("%s", repr_use, extra=log_assembled_extra)
        match log_assembled_dir:
            case None:
                path = Path.cwd()
            case Path() | str():
                path = Path(log_assembled_dir)
            case _:
                path = log_assembled_dir()
        now = (
            get_now(time_zone="local")
            .replace(tzinfo=None)
            .strftime("%Y-%m-%dT%H-%M-%S")
        )
        with writer(path.joinpath(now)) as temp, temp.open(mode="w") as fh:
            _ = fh.write(repr_use)
    async_callbacks: list[Callable[[], Coroutine1[None]]] = []
    if callbacks is not None:
        for callback in callbacks:
            if not iscoroutinefunction(callback):
                cast(Callable[[], None], callback)()
            else:
                async_callback = cast(Callable[[], Coroutine1[None]], callback)
                async_callbacks.append(async_callback)
    if len(async_callbacks) >= 1:
        run(_run_async_callbacks(async_callbacks))
    raise exc_val


@dataclass(kw_only=True, slots=True)
class MakeExceptHookError(Exception):
    @override
    def __str__(self) -> str:
        return "No exception to log"


async def _run_async_callbacks(
    callbacks: Iterable[Callable[[], Coroutine1[None]]], /
) -> None:
    """Run all asynchronous callbacks."""
    async with TaskGroup() as tg:
        for callback in callbacks:
            _ = tg.create_task(callback())


__all__ = ["VERSION_MAJOR_MINOR", "MakeExceptHookError", "make_except_hook"]
