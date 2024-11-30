from __future__ import annotations

from logging import getLogger
from os import environ, getenv
from sys import version_info
from typing import TYPE_CHECKING

from rich.pretty import pretty_repr

from utilities.traceback import yield_extended_frame_summaries

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
        return
    if not (
        (exc_type is not None) and (exc_val is not None) and (traceback is not None)
    ):
        msg = f"Exception type, exception value, and traceback must all be non-null; got {exc_type=}, {exc_val=}, {traceback=}"
        raise RuntimeError(msg)
        return
    contents = list(yield_extended_frame_summaries(exc_val, traceback=traceback))
    _LOGGER.info("%s", pretty_repr(contents))
    exc_with_tb = exc_val.with_traceback(traceback)
    this_frames = exc_val.frames
    parent = exc_val.__context__
    assert parent is not None
    parent_frames = parent.frames

    if "BREAKPOINT" in environ:
        breakpoint()


__all__ = ["VERSION_MAJOR_MINOR"]
