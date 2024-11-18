from __future__ import annotations

from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass, field
from functools import partial, wraps
from inspect import iscoroutinefunction, signature
from linecache import getline
from pathlib import Path
from sys import _getframe, exc_info, version_info
from textwrap import indent
from traceback import StackSummary, TracebackException
from typing import TYPE_CHECKING, Any, TypedDict, TypeVar, cast, overload

from typing_extensions import override

from utilities.errors import ImpossibleCaseError
from utilities.functions import ensure_not_none, get_class_name, get_func_name
from utilities.iterables import one

if TYPE_CHECKING:
    from types import FrameType, TracebackType


VERSION_MAJOR_MINOR = (version_info.major, version_info.minor)
_F = TypeVar("_F", bound=Callable[..., Any])
_MAX_WIDTH = 80
_INDENT_SIZE = 4
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
class _GetExcTraceInfoOutput:
    """A collection of exception data."""

    exc_type: type[BaseException] | None = None
    exc_value: BaseException | None = None
    frames: list[_FrameInfo] = field(default_factory=list)

    def pretty(
        self,
        *,
        location: bool = True,
        max_width: int = _MAX_WIDTH,
        indent_size: int = _INDENT_SIZE,
        max_length: int | None = None,
        max_string: int | None = None,
        max_depth: int | None = None,
        expand_all: bool = False,
    ) -> str:
        """Pretty print the exception data."""
        return "\n".join(
            self._pretty_yield(
                location=location,
                max_width=max_width,
                indent_size=indent_size,
                max_length=max_length,
                max_string=max_string,
                max_depth=max_depth,
                expand_all=expand_all,
            )
        )

    def _pretty_yield(
        self,
        /,
        *,
        location: bool = True,
        max_width: int = _MAX_WIDTH,
        indent_size: int = _INDENT_SIZE,
        max_length: int | None = None,
        max_string: int | None = None,
        max_depth: int | None = None,
        expand_all: bool = False,
    ) -> Iterable[str]:
        """Yield the rows for pretty printing the exception."""
        from rich.pretty import pretty_repr

        pretty = partial(
            pretty_repr,
            max_width=max_width,
            indent_size=indent_size,
            max_length=max_length,
            max_string=max_string,
            max_depth=max_depth,
            expand_all=expand_all,
        )

        if (self.exc_type is None) or (self.exc_value is None):  # pragma: no cover
            raise ImpossibleCaseError(case=[f"{self.exc_type=}", f"{self.exc_value=}"])
        error = f">> {self.exc_type.__name__}: {self.exc_value}"

        yield "Error running:"
        yield ""
        for frame in self.frames:
            yield indent(f"{frame.depth}. {get_func_name(frame.func)}", self._prefix1)
        yield indent(error, self._prefix1)
        yield ""
        yield "Traced frames:"
        for frame in self.frames:
            name, filename = get_func_name(frame.func), frame.filename
            yield ""
            desc = f"{name} ({filename}:{frame.first_line_num})" if location else name
            yield indent(f"{frame.depth}/{frame.max_depth}. {desc}", self._prefix1)
            for i, arg in enumerate(frame.args):
                yield indent(f"args[{i}] = {pretty(arg)}", self._prefix2)
            for k, v in frame.kwargs.items():
                yield indent(f"kwargs[{k!r}] = {pretty(v)}", self._prefix2)
            yield indent(f">> {frame.code_line}", self._prefix2)
            if location:  # pragma: no cover
                yield indent(f"   ({filename}:{frame.line_num})", self._prefix2)
            if frame.depth == frame.max_depth:
                yield indent(error, self._prefix2)

    @property
    def _prefix1(self) -> str:
        return 2 * " "

    @property
    def _prefix2(self) -> str:
        return 2 * self._prefix1


@dataclass(kw_only=True)
class _FrameInfo:
    """A collection of frame data."""

    depth: int
    max_depth: int
    func: Callable[..., Any]
    filename: Path
    first_line_num: int
    line_num: int
    code_line: str
    args: tuple[Any, ...] = field(default_factory=tuple)
    kwargs: dict[str, Any] = field(default_factory=dict)
    error: Exception | None = None


def get_exc_trace_info() -> _GetExcTraceInfoOutput:
    """Get the exception information, extracting & merging trace data if it exists."""
    type_, value, traceback = exc_info()
    raw = list(_get_exc_trace_info_yield_raw(traceback=traceback))
    merged = list(_get_exc_trace_info_yield_merged(raw))
    frames = [
        _FrameInfo(
            depth=i,
            max_depth=len(merged),
            func=f.func,
            filename=f.filename,
            first_line_num=f.first_line_num,
            line_num=f.line_num,
            code_line=f.code_line,
            args=f.args,
            kwargs=f.kwargs,
        )
        for i, f in enumerate(merged[::-1], start=1)
    ]
    return _GetExcTraceInfoOutput(exc_type=type_, exc_value=value, frames=frames)


@dataclass(kw_only=True)
class _GetExcTraceInfoRaw:
    """A collection of raw frame data."""

    func_name: str
    filename: Path
    first_line_num: int
    line_num: int
    code_line: str
    trace: _TraceData | None = None


def _get_exc_trace_info_yield_raw(
    *, traceback: TracebackType | None = None
) -> Iterator[_GetExcTraceInfoRaw]:
    """Yield the raw frame info."""
    while traceback is not None:
        frame = traceback.tb_frame
        code = frame.f_code
        yield _GetExcTraceInfoRaw(
            func_name=code.co_name,
            filename=Path(code.co_filename),
            first_line_num=code.co_firstlineno,
            line_num=traceback.tb_lineno,
            code_line=getline(code.co_filename, traceback.tb_lineno).strip(),
            trace=frame.f_locals.get(_TRACE_DATA),
        )
        traceback = traceback.tb_next


@dataclass(kw_only=True)
class _GetExcTraceInfoMerged:
    """A collection of unnumbered frame data."""

    func: Callable[..., Any]
    filename: Path
    first_line_num: int
    line_num: int
    code_line: str
    args: tuple[Any, ...] = field(default_factory=tuple)
    kwargs: dict[str, Any] = field(default_factory=dict)


def _get_exc_trace_info_yield_merged(
    raw: Iterable[_GetExcTraceInfoRaw], /
) -> Iterator[_GetExcTraceInfoMerged]:
    """Yield the merged frame info."""
    raw_rev = list(reversed(list(raw)))
    while True:
        try:
            curr = raw_rev.pop(0)
        except IndexError:  # pragma: no cover
            return
        if len(raw_rev) == 0:
            return
        next_: _GetExcTraceInfoRaw | None = None
        while (len(raw_rev) >= 1) and ((next_ is None) or (next_.trace is None)):
            next_ = raw_rev.pop(0)
        next_ = ensure_not_none(next_)
        if next_.trace is None:
            return
        yield _GetExcTraceInfoMerged(
            func=next_.trace.func,
            filename=curr.filename,
            first_line_num=curr.first_line_num,
            line_num=curr.line_num,
            code_line=curr.code_line,
            args=next_.trace.args,
            kwargs=next_.trace.kwargs,
        )
        raw_rev = raw_rev[next_.trace.above :]


@dataclass(kw_only=True, slots=True)
class _TraceData:
    """A collection of tracing data."""

    func: Callable[..., Any]
    args: tuple[Any, ...] = field(default_factory=tuple)
    kwargs: dict[str, Any] = field(default_factory=dict)
    above: int = 0
    below: int = 0


@dataclass(kw_only=True, slots=True)
class _TraceDataWithStack(_TraceData):
    """A collection of tracing data."""

    stack: StackSummary


@dataclass(kw_only=True, slots=False)  # no slots
class _TraceDataMixin:
    """A collection of tracing data."""

    error: Exception
    trace_data: list[_TraceDataWithStack] = field(default_factory=list)
    location: bool = True
    max_width: int = _MAX_WIDTH
    indent_size: int = _INDENT_SIZE
    max_length: int | None = None
    max_string: int | None = None
    max_depth: int | None = None
    expand_all: bool = False

    @property
    def formatted(self) -> list[Final]:
        return [
            _convert_trace_data_with_stack_into_final(i, len(self.trace_data), data)
            for i, data in enumerate(self.trace_data[::-1], start=1)
        ]

    @override
    def __repr__(self) -> str:
        """Pretty print the exception data."""
        return "\n".join(self._yield_repr_lines())

    def _yield_repr_lines(
        self,
        /,
        *,
        location: bool = True,
        max_width: int = _MAX_WIDTH,
        indent_size: int = _INDENT_SIZE,
        max_length: int | None = None,
        max_string: int | None = None,
        max_depth: int | None = None,
        expand_all: bool = False,
    ) -> Iterable[str]:
        """Yield the rows for pretty printing the exception."""
        from rich.pretty import pretty_repr

        pretty = partial(
            pretty_repr,
            max_width=max_width,
            indent_size=indent_size,
            max_length=max_length,
            max_string=max_string,
            max_depth=max_depth,
            expand_all=expand_all,
        )

        error = f">> {get_class_name(self.error)}: {self.error}"

        yield "Error running:"
        yield ""
        for frame in self.formatted:
            yield indent(f"{frame.depth}. {get_func_name(frame.func)}", self._prefix1)
        # yield indent(error, self._prefix1)
        yield ""
        yield "Traced frames:"
        for frame in self.formatted:
            name, filename = get_func_name(frame.func), frame.filename
            yield ""
            desc = f"{name} ({filename}:{frame.first_line_num})" if location else name
            yield indent(f"{frame.depth}/{frame.max_depth}. {desc}", self._prefix1)
            for i, arg in enumerate(frame.args):
                yield indent(f"args[{i}] = {pretty(arg)}", self._prefix2)
            for k, v in frame.kwargs.items():
                yield indent(f"kwargs[{k!r}] = {pretty(v)}", self._prefix2)
            yield indent(f">> {frame.line}", self._prefix2)
            if location:  # pragma: no cover
                yield indent(f"   ({filename}:{frame.line_num})", self._prefix2)
            if frame.depth == frame.max_depth:
                yield indent(error, self._prefix2)

    @property
    def _prefix1(self) -> str:
        return 2 * " "

    @property
    def _prefix2(self) -> str:
        return 2 * self._prefix1


def _convert_trace_data_with_stack_into_final(
    i: int, n: int, data: _TraceDataWithStack, /
) -> Final:
    summary = one(s for s in data.stack if s.name == get_func_name(data.func))
    return Final(
        depth=i,
        max_depth=n,
        filename=Path(summary.filename),
        line_num=summary.lineno,
        end_line_num=summary.end_lineno,
        col_num=summary.colno,
        end_col_num=summary.end_colno,
        name=summary.name,
        line=summary.line,
        func=data.func,
        args=data.args,
        kwargs=data.kwargs,
    )


@dataclass(kw_only=True, slots=True)
class Final:
    depth: int
    max_depth: int
    filename: Path
    line_num: int | None = None
    end_line_num: int | None = None
    col_num: int | None = None
    end_col_num: int | None = None
    name: str
    line: str | None = None
    func: Callable[..., Any]
    args: tuple[Any, ...] = field(default_factory=tuple)
    kwargs: dict[str, Any] = field(default_factory=dict)


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
            try:
                trace_data = locals()[_TRACE_DATA] = _trace_make_data(
                    func, above, below, *args, **kwargs
                )
            except TypeError:
                return func(*args, **kwargs)
            try:
                return func(*args, **kwargs)
            except Exception as error:  # noqa: BLE001
                traceback_exception = TracebackException.from_exception(
                    error, capture_locals=True
                )
                trace_data_with_stack = _TraceDataWithStack(
                    func=trace_data.func,
                    args=trace_data.args,
                    kwargs=trace_data.kwargs,
                    above=trace_data.above,
                    below=trace_data.below,
                    stack=traceback_exception.stack,
                )
                cls = type(error)
                if isinstance(error, _TraceDataMixin):
                    bases = (cls,)
                    merged = [*error.trace_data, trace_data_with_stack]
                else:
                    bases = (cls, _TraceDataMixin)
                    merged = [trace_data_with_stack]
                raise type(cls.__name__, bases, {"trace_data": merged})(
                    *error.args
                ) from None

        return trace_sync

    @wraps(func)
    async def log_call_async(*args: Any, **kwargs: Any) -> Any:
        try:
            trace_data = locals()[_TRACE_DATA] = _trace_make_data(
                func, above, below, *args, **kwargs
            )
        except TypeError:
            return await func(*args, **kwargs)
        try:
            return await func(*args, **kwargs)
        except Exception as error:  # noqa: BLE001
            traceback_exception = TracebackException.from_exception(
                error, capture_locals=True
            )
            trace_data_with_stack = _TraceDataWithStack(
                func=trace_data.func,
                args=trace_data.args,
                kwargs=trace_data.kwargs,
                above=trace_data.above,
                below=trace_data.below,
                stack=traceback_exception.stack,
            )
            cls = type(error)
            if isinstance(error, _TraceDataMixin):
                bases = (cls,)
                merged = [*error.trace_data, trace_data_with_stack]
            else:
                bases = (cls, _TraceDataMixin)
                merged = [trace_data_with_stack]
            raise type(cls.__name__, bases, {"trace_data": merged})(
                *error.args
            ) from None

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
