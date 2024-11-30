from __future__ import annotations

from logging import getLogger
from os import environ
from sys import version_info
from typing import TYPE_CHECKING

from rich.pretty import pretty_repr

from utilities.traceback import _extra, yield_extended_frame_summaries

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
    contents = list(
        yield_extended_frame_summaries(exc_val, traceback=traceback, extra=_extra)
    )
    _LOGGER.info("%s", pretty_repr(contents))
    parent = exc_val.__context__
    assert parent is not None

    if "BREAKPOINT" in environ:
        breakpoint()


def extra(_: FrameSummary, frame: FrameType) -> _CallArgs | None:
    return frame.f_locals.get("call_args")


__all__ = ["VERSION_MAJOR_MINOR"]
