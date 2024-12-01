from __future__ import annotations

from logging import Logger, getLogger
from sys import version_info
from typing import TYPE_CHECKING

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
) -> None:
    """Exception hook to log the traceback."""
    if (exc_type is None) and (exc_val is None) and (traceback is None):
        return
    if not (
        (exc_type is not None) and (exc_val is not None) and (traceback is not None)
    ):
        msg = f"Exception type, exception value, and traceback must all be non-null; got {exc_type=}, {exc_val=}, {traceback=}"
        raise RuntimeError(msg)
    error = assemble_exception_paths(exc_val)
    try:
        from rich.pretty import pretty_repr
    except ImportError:
        repr_use = repr(error)
    else:
        repr_use = pretty_repr(error)
    logger.error("%s", repr_use)


__all__ = ["VERSION_MAJOR_MINOR"]
