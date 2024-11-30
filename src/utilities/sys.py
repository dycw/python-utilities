from __future__ import annotations

from logging import getLogger
from os import environ
from sys import version_info
from typing import TYPE_CHECKING

from utilities.traceback import (
    HasExceptionPath,
    assemble_exception_paths,
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

    assemble_exception_paths(exc_val)
    breakpoint()

    errors = [e for e in yield_exceptions(exc_val) if isinstance(e, HasExceptionPath)]
    match len(errors):
        case 1:
            errors[0].exc_path
            if isinstance(errors[0], ExceptionGroup):
                inner = [
                    e for e in errors[0].exceptions if isinstance(e, HasExceptionPath)
                ]
                from utilities.iterables import one

                one(inner).exc_path
                if "BREAKPOINT" in environ:
                    breakpoint()
            elif "BREAKPOINT" in environ:
                breakpoint()
            return am
        case 2:
            a, b = errors
            am, bm = a.exc_path.frames, b.exc_path.frames
            if "BREAKPOINT" in environ:
                breakpoint()
            return am, bm
    return None


__all__ = ["VERSION_MAJOR_MINOR"]
