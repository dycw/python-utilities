from __future__ import annotations

from dataclasses import dataclass
from sys import exc_info
from traceback import TracebackException
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterator
    from types import FrameType, TracebackType


@dataclass(kw_only=True, slots=True)
class ExtStackSummary(list):
    pass


def get_extended_stack_summary(error: Exception, /) -> None:
    """Get the extended stack summary."""
    tb_exc = TracebackException.from_exception(error, capture_locals=True)
    tb_exc.stack[0]
    _, _, _tb = exc_info()


def yield_frames(*, traceback: TracebackType | None = None) -> Iterator[FrameType]:
    """Yield the frames of a traceback."""
    while traceback is not None:
        yield traceback.tb_frame
        traceback = traceback.tb_next


__all__ = ["yield_frames"]
