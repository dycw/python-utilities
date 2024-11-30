from __future__ import annotations

from logging import getLogger
from os import environ
from sys import version_info
from typing import TYPE_CHECKING

from rich.pretty import pretty_repr

from utilities.traceback import (
    HasCallArgFrameSummaries,
    get_frame_summaries_with_call_args,
    yield_exceptions,
)

if TYPE_CHECKING:
    from types import TracebackType

_LOGGER = getLogger(__name__)
VERSION_MAJOR_MINOR = (version_info.major, version_info.minor)


def log_traceback_excepthook(
    exc_type: type[BaseException] | None,
    exc_val: BaseException | None,
    traceback: TracebackType | None,
    /,
) -> None:
    """Exception hook to log the traceback."""
    if (exc_type is None) and (exc_val is None) and (traceback is None):
        return None
    if not (
        (exc_type is not None) and (exc_val is not None) and (traceback is not None)
    ):
        msg = f"Exception type, exception value, and traceback must all be non-null; got {exc_type=}, {exc_val=}, {traceback=}"
        raise RuntimeError(msg)
    contents = get_frame_summaries_with_call_args(exc_val, traceback=traceback)
    _LOGGER.info("%s", pretty_repr(contents))
    errors = [
        e for e in yield_exceptions(exc_val) if isinstance(e, HasCallArgFrameSummaries)
    ]
    a, b = errors
    am, bm = a.frames.merge(), b.frames.merge()
    if "BREAKPOINT" in environ:
        breakpoint()

    return am, bm


__all__ = ["VERSION_MAJOR_MINOR"]
