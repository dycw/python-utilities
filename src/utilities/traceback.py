from __future__ import annotations

from dataclasses import dataclass
from sys import exc_info
from traceback import TracebackException
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterator
    from types import FrameType, TracebackType


@dataclass(kw_only=True, slots=True)
class ExtFrameSummary:
    """An extended frame summary."""

    filename: str
    lineno: int | None = None
    end_lineno: int | None = None
    colno: int | None = None
    end_colno: int | None = None
    name: str
    qualname: str
    line: str | None = None
    locals: dict[str, str] | None = None


def yield_extended_frame_summaries(
    error: Exception, /, *, traceback: TracebackType | None = None
) -> Iterator[ExtFrameSummary]:
    """Yield the extended frame summaries."""
    tb_exc = TracebackException.from_exception(error, capture_locals=True)
    if traceback is None:
        _, _, traceback_use = exc_info()
    else:
        traceback_use = traceback
    frames = yield_frames(traceback=traceback_use)
    for summary, frame in zip(tb_exc.stack, frames, strict=True):
        yield ExtFrameSummary(
            filename=summary.filename,
            lineno=summary.lineno,
            end_lineno=summary.end_lineno,
            colno=summary.colno,
            end_colno=summary.end_colno,
            name=summary.name,
            qualname=frame.f_code.co_qualname,
            line=summary.line,
            locals=summary.locals,
        )


def yield_frames(*, traceback: TracebackType | None = None) -> Iterator[FrameType]:
    """Yield the frames of a traceback."""
    while traceback is not None:
        yield traceback.tb_frame
        traceback = traceback.tb_next


__all__ = ["yield_extended_frame_summaries", "yield_frames"]
