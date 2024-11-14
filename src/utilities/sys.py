from __future__ import annotations

from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass, field
from functools import partial, wraps
from inspect import iscoroutinefunction, signature
from itertools import pairwise
from pathlib import Path
from sys import _getframe, exc_info, version_info
from typing import TYPE_CHECKING, Any, TypedDict, TypeVar, cast, overload

from rich.pretty import pretty_repr

from utilities.functions import ensure_not_none
from utilities.iterables import OneEmptyError, OneNonUniqueError, one
from utilities.sentinel import Sentinel, sentinel

if TYPE_CHECKING:
    from types import FrameType, TracebackType

    from utilities.sentinel import Sentinel


VERSION_MAJOR_MINOR = (version_info.major, version_info.minor)
_F = TypeVar("_F", bound=Callable[..., Any])
_TRACE_DATA = "_TRACE_DATA"


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


def get_exc_trace_info() -> _GetExceptionOutput:
    """Get the exception information, extracting trace data if it exists."""
    type_, value, traceback = exc_info()
    raw = list(_get_exc_trace_info_yield_raw(traceback=traceback))
    merged = list(_get_exc_trace_info_yield_merged(raw))
    frames = [
        _FrameInfo(
            depth=i,
            max_depth=len(merged),
            filename=f.filename,
            first_line_num=f.first_line_num,
            line_num=f.line_num,
            func=f.func,
            args=f.args,
            kwargs=f.kwargs,
            result=f.result,
            error=f.error,
        )
        for i, f in enumerate(merged[::-1], start=1)
    ]
    return _GetExceptionOutput(exc_type=type_, exc_value=value, frames=frames)


@dataclass(kw_only=True)
class _GetExcTraceInfoRaw:
    """A collection of raw frame data."""

    filename: Path
    first_line_num: int
    line_num: int
    func_name: str
    trace: _TraceData | None = None


def _get_exc_trace_info_yield_raw(
    *, traceback: TracebackType | None = None
) -> Iterator[_GetExcTraceInfoRaw]:
    """Yield the raw frame info."""
    while traceback is not None:
        frame = traceback.tb_frame
        code = frame.f_code
        yield _GetExcTraceInfoRaw(
            filename=Path(code.co_filename),
            first_line_num=code.co_firstlineno,
            line_num=traceback.tb_lineno,
            func_name=code.co_name,
            trace=frame.f_locals.get(_TRACE_DATA),
        )
        traceback = traceback.tb_next


@dataclass(kw_only=True)
class _GetExcTraceInfoMerged:
    """A collection of unnumbered frame data."""

    filename: Path
    first_line_num: int
    line_num: int
    func: Callable[..., Any]
    args: tuple[Any, ...] = field(default_factory=tuple)
    kwargs: dict[str, Any] = field(default_factory=dict)
    result: Any | Sentinel = sentinel
    error: Exception | None = None


def _get_exc_trace_info_yield_merged(
    raw: Iterable[_GetExcTraceInfoRaw], /
) -> Iterator[_GetExcTraceInfoMerged]:
    """Yield the merged frame info."""
    raw_rev = list(reversed(list(raw)))
    while len(raw_rev) >= 1:
        try:
            curr = raw_rev.pop(0)
        except IndexError:
            return
        next_: _GetExcTraceInfoRaw | None = None
        if len(raw_rev) == 0:
            return
        while (len(raw_rev) >= 1) and ((next_ is None) or (next_.trace is None)):
            try:
                next_ = raw_rev.pop(0)
            except IndexError:
                return
        next_trace = ensure_not_none(ensure_not_none(next_).trace)
        yield _GetExcTraceInfoMerged(
            filename=curr.filename,
            first_line_num=curr.first_line_num,
            line_num=curr.line_num,
            func=next_trace.func,
            args=next_trace.args,
            kwargs=next_trace.kwargs,
            result=next_trace.result,
            error=next_trace.error,
        )
        raw_rev = raw_rev[next_trace.above :]


@dataclass(kw_only=True, slots=True)
class _TraceData:
    """A collection of tracing data."""

    func: Callable[..., Any]
    args: tuple[Any, ...] = field(default_factory=tuple)
    kwargs: dict[str, Any] = field(default_factory=dict)
    above: int = 0
    below: int = 0
    result: Any | Sentinel = sentinel
    error: Exception | None = None


@overload
def trace(func: _F, /, *, above: int = ..., below: int = ...) -> _F: ...
@overload
def trace(
    func: None = None, /, *, above: int = ..., below: int = ...
) -> Callable[[_F], _F]: ...
def trace(
    func: _F | None = None, /, *, above: int = 0, below: int = 0
) -> _F | Callable[[_F], _F]:
    """Trace a function call."""
    if func is None:
        result = partial(trace, above=above, below=below)
        return cast(Callable[[_F], _F], result)

    if not iscoroutinefunction(func):

        @wraps(func)
        def trace_sync(*args: Any, **kwargs: Any) -> Any:
            trace_data = _trace_make_data(func, above, below, *args, **kwargs)
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
        trace_data = _trace_make_data(func, above, below, *args, **kwargs)
        try:
            result = trace_data.result = await func(*args, **kwargs)
        except Exception as error:
            trace_data.error = error
            locals()[_TRACE_DATA] = trace_data
            raise
        locals()[_TRACE_DATA] = trace_data
        return result

    return cast(_F, log_call_async)


def _trace_make_data(
    func: Callable[..., Any], above: int = 0, below: int = 0, *args: Any, **kwargs: Any
) -> _TraceData:
    """Make the initial trace data."""
    bound_args = signature(func).bind(*args, **kwargs)
    return _TraceData(
        func=func,
        args=bound_args.args,
        kwargs=bound_args.kwargs,
        above=above,
        below=below,
    )


__all__ = ["VERSION_MAJOR_MINOR", "get_caller", "get_exc_trace_info", "trace"]
