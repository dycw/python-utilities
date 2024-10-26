from __future__ import annotations

from sys import _getframe, version_info
from typing import TYPE_CHECKING, TypedDict

if TYPE_CHECKING:
    from types import FrameType

VERSION_MAJOR_MINOR = (version_info.major, version_info.minor)


class _GetCallerOutput(TypedDict):
    module: str
    line_num: int
    name: str


def get_caller(*, depth: int = 2) -> _GetCallerOutput:
    """Get the calling function."""
    i = 0
    frame: FrameType | None = _getframe()  # pragma: no cover
    while (i < depth) and (frame.f_back is not None):
        i, frame = i + 1, frame.f_back
    return {
        "module": frame.f_globals["__name__"],
        "line_num": frame.f_lineno,
        "name": frame.f_code.co_name,
    }


__all__ = ["VERSION_MAJOR_MINOR", "get_caller"]
