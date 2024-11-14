from __future__ import annotations

from dataclasses import dataclass, field
from inspect import formatargvalues, getargvalues
from pathlib import Path
from sys import _getframe, exc_info, version_info
from typing import TYPE_CHECKING, Any, TypedDict

if TYPE_CHECKING:
    from types import FrameType, TracebackType

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


@dataclass(kw_only=True)
class _LogExceptionOutput:
    exc_type: type[BaseException] | None = None
    exc_value: BaseException | None = None
    frames: list[_FrameInfo] = field(default_factory=list)


@dataclass(kw_only=True)
class _FrameInfo:
    filename: Path
    line_num: int
    func_name: str
    args: tuple[Any, ...] = field(default_factory=tuple)
    kwargs: dict[str, Any] = field(default_factory=dict)


def get_exception_info() -> _LogExceptionOutput:
    exc_type, exc_value, exc_traceback = exc_info()
    frame_infos: list[_FrameInfo] = []
    traceback: TracebackType | None = exc_traceback
    while traceback is not None:
        frame = traceback.tb_frame
        code = frame.f_code
        arg_info = getargvalues(frame)
        frame_infos.append(
            _FrameInfo(
                filename=Path(code.co_filename),
                line_num=traceback.tb_lineno,
                func_name=code.co_name,
                args=arg_info.varargs,
                kwargs=arg_info.keywords,
            )
        )
        breakpoint()

        traceback = traceback.tb_next
    return _LogExceptionOutput(
        exc_type=exc_type, exc_value=exc_value, frames=frame_infos
    )


__all__ = ["VERSION_MAJOR_MINOR", "get_caller", "get_exception_info"]
