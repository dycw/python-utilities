from __future__ import annotations

from dataclasses import dataclass
from logging import Logger, getLogger
from sys import version_info
from typing import TYPE_CHECKING

from typing_extensions import override

from utilities.traceback import assemble_exception_paths

if TYPE_CHECKING:
    from types import TracebackType

_LOGGER = getLogger(__name__)
VERSION_MAJOR_MINOR = (version_info.major, version_info.minor)


def log_exception_paths(
    exc_type: type[BaseException] | None,
    exc_val: BaseException | None,
    traceback: TracebackType | None,
    /,
    *,
    logger: Logger = _LOGGER,
    max_width: int = 80,
    indent_size: int = 4,
    max_length: int | None = None,
    max_string: int | None = None,
    max_depth: int | None = None,
    expand_all: bool = False,
) -> None:
    """Exception hook to log the traceback."""
    _ = (exc_type, traceback)
    if exc_val is None:  # pragma: no cover
        raise LogExceptionPathsError
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
    logger.error("%s", repr_use)


@dataclass(kw_only=True, slots=True)
class LogExceptionPathsError(Exception):
    @override
    def __str__(self) -> str:
        return "No exception to log"


__all__ = ["VERSION_MAJOR_MINOR", "LogExceptionPathsError", "log_exception_paths"]
