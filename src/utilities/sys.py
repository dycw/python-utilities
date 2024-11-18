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


@dataclass(kw_only=True, slots=True)
class _TraceData:
    """A collection of tracing data."""

    func: Callable[..., Any]
    args: tuple[Any, ...] = field(default_factory=tuple)
    kwargs: dict[str, Any] = field(default_factory=dict)


@dataclass(kw_only=True, slots=True)
class _TraceDataWithStack(_TraceData):
    """A collection of tracing data."""

    stack: StackSummary


@dataclass(kw_only=True, slots=False)  # no slots
class _TraceDataMixin:
    """A collection of tracing data."""

    error: Exception
    trace_data: list[_TraceDataWithStack] = field(default_factory=list)

    @property
    def formatted(self) -> list[Final]:
        return [
            _convert_trace_data_with_stack_into_final(i, len(self.trace_data), data)
            for i, data in enumerate(self.trace_data[::-1], start=1)
        ]

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
            self._yield_repr_lines(
                location=location,
                max_width=max_width,
                indent_size=indent_size,
                max_length=max_length,
                max_string=max_string,
                max_depth=max_depth,
                expand_all=expand_all,
            )
        )

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
        yield indent(error, self._prefix1)
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
            try:
                trace_data = locals()[_TRACE_DATA] = _trace_make_data(
                    func, *args, **kwargs
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
                    stack=traceback_exception.stack,
                )
                cls = type(error)
                if isinstance(error, _TraceDataMixin):
                    bases = (cls,)
                    merged = [*error.trace_data, trace_data_with_stack]
                else:
                    bases = (cls, _TraceDataMixin)
                    merged = [trace_data_with_stack]
                raise type(cls.__name__, bases, {"error": error, "trace_data": merged})(
                    *error.args
                ) from None

        return trace_sync

    @wraps(func)
    async def log_call_async(*args: Any, **kwargs: Any) -> Any:
        try:
            trace_data = locals()[_TRACE_DATA] = _trace_make_data(func, *args, **kwargs)
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


def _trace_make_data(func: Callable[..., Any], *args: Any, **kwargs: Any) -> _TraceData:
    """Make the initial trace data."""
    bound_args = signature(func).bind(*args, **kwargs)
    return _TraceData(
        func=func,
        args=bound_args.args,
        kwargs=bound_args.kwargs,
    )


__all__ = ["VERSION_MAJOR_MINOR", "get_caller", "trace"]
