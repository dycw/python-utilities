from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from functools import partial, wraps
from inspect import iscoroutinefunction, signature
from itertools import pairwise
from pathlib import Path
from sys import _getframe, exc_info, version_info
from typing import TYPE_CHECKING, Any, TypedDict, TypeVar, cast, overload

from utilities.sentinel import Sentinel, sentinel

if TYPE_CHECKING:
    from types import FrameType, TracebackType

    from utilities.sentinel import Sentinel

_TRACE_DATA = "_TRACE_DATA"
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
class _GetExceptionOutput:
    """A collection of exception data."""

    exc_type: type[BaseException] | None = None
    exc_value: BaseException | None = None
    frames: list[_FrameInfo] = field(default_factory=list)


@dataclass(kw_only=True)
class _FrameInfo:
    """A collection of frame data."""

    depth: int
    max_depth: int
    filename: Path
    first_line_num: int
    line_num: int
    func: Callable[..., Any]
    args: tuple[Any, ...] = field(default_factory=tuple)
    kwargs: dict[str, Any] = field(default_factory=dict)
    result: Any | Sentinel = sentinel
    error: Exception | None = None


@dataclass(kw_only=True)
class _UnnumberedFrameInfo:
    """A collection of unnumbered frame data."""

    filename: Path
    first_line_num: int
    line_num: int
    func: Callable[..., Any]
    args: tuple[Any, ...] = field(default_factory=tuple)
    kwargs: dict[str, Any] = field(default_factory=dict)
    result: Any | Sentinel = sentinel
    error: Exception | None = None


@dataclass(kw_only=True)
class _RawFrameInfo:
    """A collection of raw frame data."""

    filename: Path
    first_line_num: int
    line_num: int
    func_name: str
    trace: _TraceData | None = None


def get_exc_trace_info() -> _GetExceptionOutput:
    """Get the exception information, extracting trace data if it exists."""
    exc_type, exc_value, exc_traceback = exc_info()
    raw_frame_infos: list[_RawFrameInfo] = []
    traceback: TracebackType | None = exc_traceback
    while traceback is not None:
        frame = traceback.tb_frame
        code = frame.f_code
        raw_frame_info = _RawFrameInfo(
            filename=Path(code.co_filename),
            first_line_num=code.co_firstlineno,
            line_num=traceback.tb_lineno,
            func_name=code.co_name,
            trace=frame.f_locals.get(_TRACE_DATA),
        )
        raw_frame_infos.append(raw_frame_info)
        traceback = traceback.tb_next
    unnumbered_frame_infos: list[_UnnumberedFrameInfo] = []
    for curr, next_ in pairwise(reversed(raw_frame_infos)):
        if next_.trace is not None:
            unnumbered_frame_info = _UnnumberedFrameInfo(
                filename=curr.filename,
                first_line_num=curr.first_line_num,
                line_num=curr.line_num,
                func=next_.trace.func,
                args=next_.trace.args,
                kwargs=next_.trace.kwargs,
                result=next_.trace.result,
                error=next_.trace.error,
            )
            unnumbered_frame_infos.append(unnumbered_frame_info)
    frame_infos = [
        _FrameInfo(
            depth=i,
            max_depth=len(unnumbered_frame_infos),
            filename=f.filename,
            first_line_num=f.first_line_num,
            line_num=f.line_num,
            func=f.func,
            args=f.args,
            kwargs=f.kwargs,
            result=f.result,
            error=f.error,
        )
        for i, f in enumerate(unnumbered_frame_infos[::-1], start=1)
    ]
    return _GetExceptionOutput(
        exc_type=exc_type, exc_value=exc_value, frames=frame_infos
    )


_F = TypeVar("_F", bound=Callable[..., Any])


@dataclass(kw_only=True, slots=True)
class _TraceData:
    """A collection of tracing data."""

    func: Callable[..., Any]
    args: tuple[Any, ...] = field(default_factory=tuple)
    kwargs: dict[str, Any] = field(default_factory=dict)
    result: Any | Sentinel = sentinel
    error: Exception | None = None


@overload
def trace(func: _F, /) -> _F: ...
@overload
def trace(func: None = None, /) -> Callable[[_F], _F]: ...
def trace(func: _F | None = None, /) -> _F | Callable[[_F], _F]:
    """Trace a function call."""
    if func is None:
        result = partial(trace)
        return cast(Callable[[_F], _F], result)

    if not iscoroutinefunction(func):

        @wraps(func)
        def trace_sync(*args: Any, **kwargs: Any) -> Any:
            trace_data = _trace_make_data(func, *args, **kwargs)
            try:
                result = trace_data.result = func(*args, **kwargs)
            except Exception as error:
                trace_data.error = error
                locals()[_TRACE_DATA] = trace_data
                raise
            locals()[_TRACE_DATA] = trace_data
            return result

        return trace_sync

    @wraps(func)
    async def log_call_async(*args: Any, **kwargs: Any) -> Any:
        trace_data = _trace_make_data(func, *args, **kwargs)
        try:
            result = trace_data.result = await func(*args, **kwargs)
        except Exception as error:
            trace_data.error = error
            locals()[_TRACE_DATA] = trace_data
            raise
        locals()[_TRACE_DATA] = trace_data
        return result

    return cast(_F, log_call_async)


def _trace_make_data(func: Callable[..., Any], *args: Any, **kwargs: Any) -> _TraceData:
    """Make the initial trace data."""
    bound_args = signature(func).bind(*args, **kwargs)
    return _TraceData(func=func, args=bound_args.args, kwargs=bound_args.kwargs)


__all__ = ["VERSION_MAJOR_MINOR", "get_caller", "get_exc_trace_info", "trace"]
