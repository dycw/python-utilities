from __future__ import annotations

from dataclasses import dataclass
from functools import partial
from logging import Logger, getLogger
from pathlib import Path
from sys import version_info
from typing import TYPE_CHECKING

from typing_extensions import override

from utilities.atomicwrites import writer
from utilities.datetime import get_now
from utilities.logging import get_default_logging_path
from utilities.traceback import assemble_exception_paths

if TYPE_CHECKING:
    from collections.abc import Callable
    from types import TracebackType

    from utilities.types import PathLike, StrMapping

_LOGGER = getLogger(__name__)
VERSION_MAJOR_MINOR = (version_info.major, version_info.minor)


def _get_default_logging_path() -> Path:
    """Get the logging default path."""
    return get_default_logging_path().joinpath("errors")


def log_exception_paths(
    *,
    logger: Logger = _LOGGER,
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
) -> Callable[
    [type[BaseException] | None, BaseException | None, TracebackType | None], None
]:
    """Exception hook to log the traceback."""
    return partial(
        _log_exception_paths_inner,
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
    )


def _log_exception_paths_inner(
    exc_type: type[BaseException] | None,
    exc_val: BaseException | None,
    traceback: TracebackType | None,
    /,
    *,
    logger: Logger = _LOGGER,
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
) -> None:
    """Exception hook to log the traceback."""
    _ = (exc_type, traceback)
    if exc_val is None:
        raise LogExceptionPathsError
    if log_raw:
        _LOGGER.error("%s", exc_val, extra=log_raw_extra)
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


@dataclass(kw_only=True, slots=True)
class LogExceptionPathsError(Exception):
    @override
    def __str__(self) -> str:
        return "No exception to log"


__all__ = ["VERSION_MAJOR_MINOR", "LogExceptionPathsError", "log_exception_paths"]
