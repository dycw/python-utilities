from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import dataclass, field, replace
from functools import partial, wraps
from getpass import getuser
from inspect import iscoroutinefunction, signature
from logging import Formatter, LogRecord
from pathlib import Path
from socket import gethostname
from sys import exc_info
from textwrap import indent
from traceback import FrameSummary, TracebackException, format_exception
from types import TracebackType
from typing import (
    TYPE_CHECKING,
    Any,
    Generic,
    Protocol,
    Self,
    TypeAlias,
    TypeGuard,
    TypeVar,
    assert_never,
    cast,
    overload,
    runtime_checkable,
)

from typing_extensions import override

from utilities.datetime import get_now
from utilities.errors import ImpossibleCaseError
from utilities.functions import (
    ensure_not_none,
    get_class_name,
    get_func_name,
    get_func_qualname,
)
from utilities.hatch import get_version
from utilities.iterables import one
from utilities.rich import (
    EXPAND_ALL,
    INDENT_SIZE,
    MAX_DEPTH,
    MAX_LENGTH,
    MAX_STRING,
    MAX_WIDTH,
    yield_call_args_repr,
    yield_mapping_repr,
)
from utilities.text import ensure_str
from utilities.whenever import serialize_zoned_datetime

if TYPE_CHECKING:
    from collections.abc import Iterator
    from logging import _FormatStyle
    from types import FrameType

    from utilities.types import StrMapping


_F = TypeVar("_F", bound=Callable[..., Any])
_T = TypeVar("_T")
_TExc = TypeVar("_TExc", bound=BaseException)
_CALL_ARGS = "_CALL_ARGS"
_INDENT = 4 * " "
ExcInfo: TypeAlias = tuple[type[BaseException], BaseException, TracebackType]
OptExcInfo: TypeAlias = ExcInfo | tuple[None, None, None]


class RichTracebackFormatter(Formatter):
    """Formatter for rich tracebacks."""

    @override
    def __init__(
        self,
        fmt: str | None = None,
        datefmt: str | None = None,
        style: _FormatStyle = "%",
        validate: bool = True,
        /,
        *,
        defaults: StrMapping | None = None,
        detail: bool = False,
        post: Callable[[str], str] | None = None,
    ) -> None:
        super().__init__(fmt, datefmt, style, validate, defaults=defaults)
        self._detail = detail
        self._post = post

    @override
    def format(self, record: LogRecord) -> str:
        """Format the record."""
        if record.exc_info is None:
            return f"ERROR: {record.exc_info=}"
        _, exc_value, _ = record.exc_info
        if exc_value is None:
            return f"ERROR: {exc_value=}"
        error = get_rich_traceback(exc_value)
        match error:
            case ExcChainTB() | ExcGroupTB() | ExcTB():
                text = error.format(header=True, detail=self._detail)
            case BaseException():
                text = "\n".join(format_exception(error))
            case _ as never:  # pyright: ignore[reportUnnecessaryComparison]
                assert_never(never)
        if self._post is not None:
            text = self._post(text)
        return text


@dataclass(repr=False, kw_only=True, slots=True)
class _CallArgs:
    """A collection of call arguments."""

    func: Callable[..., Any]
    args: tuple[Any, ...] = field(default_factory=tuple)
    kwargs: dict[str, Any] = field(default_factory=dict)

    @override
    def __repr__(self) -> str:
        cls = get_class_name(self)
        parts: list[tuple[str, Any]] = [
            ("func", get_func_qualname(self.func)),
            ("args", self.args),
            ("kwargs", self.kwargs),
        ]
        joined = ", ".join(f"{k}={v!r}" for k, v in parts)
        return f"{cls}({joined})"

    @classmethod
    def create(cls, func: Callable[..., Any], *args: Any, **kwargs: Any) -> Self:
        """Make the initial trace data."""
        sig = signature(func)
        try:
            bound_args = sig.bind(*args, **kwargs)
        except TypeError as error:
            orig = ensure_str(one(error.args))
            lines: list[str] = [
                f"Unable to bind arguments for {get_func_name(func)!r}; {orig}"
            ]
            lines.extend(yield_call_args_repr(*args, **kwargs))
            new = "\n".join(lines)
            raise _CallArgsError(new) from None
        return cls(func=func, args=bound_args.args, kwargs=bound_args.kwargs)


class _CallArgsError(TypeError):
    """Raised when a set of call arguments cannot be created."""


@dataclass(kw_only=True, slots=True)
class _ExtFrameSummary(Generic[_T]):
    """An extended frame summary."""

    filename: Path
    module: str | None = None
    name: str
    qualname: str
    code_line: str
    first_line_num: int
    line_num: int
    end_line_num: int
    col_num: int | None = None
    end_col_num: int | None = None
    locals: dict[str, Any] = field(default_factory=dict)
    extra: _T


_ExtFrameSummaryCAOpt: TypeAlias = _ExtFrameSummary[_CallArgs | None]
_ExtFrameSummaryCA: TypeAlias = _ExtFrameSummary[_CallArgs]


@dataclass(repr=False, kw_only=True, slots=True)
class _ExcTBInternal:
    """A rich traceback for an exception; internal use only."""

    raw: list[_ExtFrameSummaryCAOpt] = field(default_factory=list)
    frames: list[_ExtFrameSummaryCA] = field(default_factory=list)
    error: BaseException


@runtime_checkable
class _HasExceptionPath(Protocol):
    @property
    def exc_tb(self) -> _ExcTBInternal: ...  # pragma: no cover


@dataclass(kw_only=True, slots=True)
class ExcChainTB(Generic[_TExc]):
    """A rich traceback for an exception chain."""

    errors: list[ExcGroupTB[_TExc] | ExcTB[_TExc] | BaseException] = field(
        default_factory=list
    )

    def __iter__(self) -> Iterator[ExcGroupTB[_TExc] | ExcTB[_TExc] | BaseException]:
        yield from self.errors

    def __len__(self) -> int:
        return len(self.errors)

    @override
    def __repr__(self) -> str:
        return self.format(header=True, detail=True)

    def format(
        self,
        *,
        header: bool = False,
        detail: bool = False,
        max_width: int = MAX_WIDTH,
        indent_size: int = INDENT_SIZE,
        max_length: int | None = MAX_LENGTH,
        max_string: int | None = MAX_STRING,
        max_depth: int | None = MAX_DEPTH,
        expand_all: bool = EXPAND_ALL,
    ) -> str:
        """Format the traceback."""
        lines: list[str] = []
        if header:
            lines.extend(_yield_header_lines())
        total = len(self.errors)
        for i, errors in enumerate(self.errors):
            lines.append(f"Exception chain {i + 1}/{total}:")
            match errors:
                case ExcGroupTB():  # pragma: no cover
                    lines.append(
                        errors.format(
                            index=i,
                            total=total,
                            header=False,
                            detail=detail,
                            max_width=max_width,
                            indent_size=indent_size,
                            max_length=max_length,
                            max_string=max_string,
                            max_depth=max_depth,
                            expand_all=expand_all,
                            depth=1,
                        )
                    )
                case ExcTB():  # pragma: no cover
                    lines.append(
                        errors.format(
                            header=False,
                            detail=detail,
                            max_width=max_width,
                            indent_size=indent_size,
                            max_length=max_length,
                            max_string=max_string,
                            max_depth=max_depth,
                            expand_all=expand_all,
                            depth=1,
                        )
                    )
                case BaseException():  # pragma: no cover
                    lines.append(_format_exception(errors, depth=2))
                case _ as never:  # pyright: ignore[reportUnnecessaryComparison]
                    assert_never(never)
            lines.append("")
        return "\n".join(lines)


@dataclass(kw_only=True, slots=True)
class ExcGroupTB(Generic[_TExc]):
    """A rich traceback for an exception group."""

    path: ExcTB[_TExc]
    errors: list[ExcGroupTB[_TExc] | ExcTB[_TExc] | BaseException] = field(
        default_factory=list
    )

    @override
    def __repr__(self) -> str:
        return self.format(header=True, detail=True)

    def format(
        self,
        *,
        index: int = 0,
        total: int = 1,
        header: bool = False,
        detail: bool = False,
        max_width: int = MAX_WIDTH,
        indent_size: int = INDENT_SIZE,
        max_length: int | None = MAX_LENGTH,
        max_string: int | None = MAX_STRING,
        max_depth: int | None = MAX_DEPTH,
        expand_all: bool = EXPAND_ALL,
        depth: int = 0,
    ) -> str:
        """Format the traceback."""
        lines: list[str] = []
        if header:
            lines.extend(_yield_header_lines())
        lines.extend([
            f"Exception group {index + 1}/{total}:",
            indent("Path:", _INDENT),
            self.path.format(header=False, detail=detail, depth=2),
            "",
        ])
        total_sub_errors = len(self.errors)
        for i, errors in enumerate(self.errors):
            lines.append(indent(f"Group error {i + 1}/{total_sub_errors}:", _INDENT))
            match errors:
                case ExcGroupTB() | ExcTB():  # pragma: no cover
                    lines.append(
                        errors.format(
                            header=False,
                            detail=detail,
                            max_width=max_width,
                            indent_size=indent_size,
                            max_length=max_length,
                            max_string=max_string,
                            max_depth=max_depth,
                            expand_all=expand_all,
                            depth=2,
                        )
                    )
                case BaseException():  # pragma: no cover
                    lines.append(_format_exception(errors, depth=2))
                case _ as never:  # pyright: ignore[reportUnnecessaryComparison]
                    assert_never(never)
            lines.append("")
        return indent("\n".join(lines), depth * _INDENT)


@dataclass(kw_only=True, slots=True)
class ExcTB(Generic[_TExc]):
    """A rich traceback for a single exception."""

    frames: list[_Frame] = field(default_factory=list)
    error: _TExc

    def __iter__(self) -> Iterator[_Frame]:
        yield from self.frames

    def __len__(self) -> int:
        return len(self.frames)

    @override
    def __repr__(self) -> str:
        return self.format(header=True, detail=True)

    def format(
        self,
        *,
        header: bool = False,
        detail: bool = False,
        max_width: int = MAX_WIDTH,
        indent_size: int = INDENT_SIZE,
        max_length: int | None = MAX_LENGTH,
        max_string: int | None = MAX_STRING,
        max_depth: int | None = MAX_DEPTH,
        expand_all: bool = EXPAND_ALL,
        depth: int = 0,
    ) -> str:
        """Format the traceback."""
        total = len(self)
        lines: list[str] = []
        if header:
            lines.extend(_yield_header_lines())
        for i, frame in enumerate(self.frames):
            is_head = i < total - 1
            lines.append(
                frame.format(
                    index=i,
                    total=total,
                    detail=detail,
                    error=None if is_head else self.error,
                    max_width=max_width,
                    indent_size=indent_size,
                    max_length=max_length,
                    max_string=max_string,
                    max_depth=max_depth,
                    expand_all=expand_all,
                )
            )
            if is_head:
                lines.append("")
        return indent("\n".join(lines), depth * _INDENT)


@dataclass(kw_only=True, slots=True)
class _Frame:
    module: str | None = None
    name: str
    code_line: str
    line_num: int
    args: tuple[Any, ...] = field(default_factory=tuple)
    kwargs: dict[str, Any] = field(default_factory=dict)
    locals: dict[str, Any] = field(default_factory=dict)

    @override
    def __repr__(self) -> str:
        return self.format(detail=True)

    def format(
        self,
        *,
        index: int = 0,
        total: int = 1,
        detail: bool = False,
        error: BaseException | None = None,
        depth: int = 0,
        max_width: int = MAX_WIDTH,
        indent_size: int = INDENT_SIZE,
        max_length: int | None = MAX_LENGTH,
        max_string: int | None = MAX_STRING,
        max_depth: int | None = MAX_DEPTH,
        expand_all: bool = EXPAND_ALL,
    ) -> str:
        """Format the traceback."""
        lines: list[str] = [f"Frame {index + 1}/{total}: {self.name} ({self.module})"]
        if detail:
            lines.append(indent("Inputs:", _INDENT))
            lines.extend(
                indent(line, 2 * _INDENT)
                for line in yield_call_args_repr(
                    *self.args,
                    _max_width=max_width,
                    _indent_size=indent_size,
                    _max_length=max_length,
                    _max_string=max_string,
                    _max_depth=max_depth,
                    _expand_all=expand_all,
                    **self.kwargs,
                )
            )
            lines.append(indent("Locals:", _INDENT))
            lines.extend(
                indent(line, 2 * _INDENT)
                for line in yield_mapping_repr(
                    _max_width=max_width,
                    _indent_size=indent_size,
                    _max_length=max_length,
                    _max_string=max_string,
                    _max_depth=max_depth,
                    _expand_all=expand_all,
                    **self.locals,
                )
            )
            lines.append(indent(f"Line {self.line_num}:", _INDENT))
            lines.append(indent(self.code_line, 2 * _INDENT))
        if error is not None:
            lines.append(_format_exception(error, detail=detail, depth=1))
        return indent("\n".join(lines), depth * _INDENT)


def get_rich_traceback(
    error: _TExc, /
) -> ExcChainTB[_TExc] | ExcGroupTB[_TExc] | ExcTB[_TExc] | BaseException:
    """Get a rich traceback."""
    match list(yield_exceptions(error)):
        case []:  # pragma: no cover
            raise ImpossibleCaseError(case=[f"{error}"])
        case [err]:
            err = cast(_TExc, err)
            return _get_rich_traceback_non_chain(err)
        case errors:
            errors = cast(list[_TExc], errors)
            return ExcChainTB(errors=[_get_rich_traceback_non_chain(e) for e in errors])


def _get_rich_traceback_non_chain(
    error: _TExc, /
) -> ExcTB[_TExc] | ExcGroupTB[_TExc] | BaseException:
    """Get a rich traceback, for a non-chained error."""
    if not isinstance(error, ExceptionGroup):
        return _get_rich_traceback_non_chain_non_group(error)
    path = cast(ExcTB[_TExc], _get_rich_traceback_non_chain_non_group(error))
    errors = cast(list[_TExc], error.exceptions)
    errors = list(map(_get_rich_traceback_non_chain, errors))
    return ExcGroupTB(path=path, errors=errors)


def _get_rich_traceback_non_chain_non_group(
    error: _TExc, /
) -> ExcTB[_TExc] | BaseException:
    """Get a rich traceback, for a non-chained, non-grouped error."""
    if isinstance(error, _HasExceptionPath):
        frames = [
            _Frame(
                module=f.module,
                name=f.name,
                code_line=f.code_line,
                line_num=f.line_num,
                args=f.extra.args,
                kwargs=f.extra.kwargs,
                locals=f.locals,
            )
            for f in error.exc_tb.frames
        ]
        return ExcTB(frames=frames, error=cast(_TExc, error))
    return error


@overload
def trace(
    func: _F,
    /,
    *,
    setup: Callable[[], bool] | None = ...,
    runtime: Callable[[], bool] | None = ...,
) -> _F: ...
@overload
def trace(
    func: None = None,
    /,
    *,
    setup: Callable[[], bool] | None = ...,
    runtime: Callable[[], bool] | None = ...,
) -> Callable[[_F], _F]: ...
def trace(
    func: _F | None = None,
    /,
    *,
    setup: Callable[[], bool] | None = None,
    runtime: Callable[[], bool] | None = None,
) -> _F | Callable[[_F], _F]:
    """Trace a function call."""
    if func is None:
        result = partial(trace, setup=setup, runtime=runtime)
        return cast(Callable[[_F], _F], result)

    if (setup is not None) and not setup():
        return func

    if runtime is None:
        if not iscoroutinefunction(func):

            @wraps(func)
            def trace_sync(*args: Any, **kwargs: Any) -> Any:
                locals()[_CALL_ARGS] = _CallArgs.create(func, *args, **kwargs)
                try:
                    return func(*args, **kwargs)
                except Exception as error:
                    cast(Any, error).exc_tb = _get_rich_traceback_internal(error)
                    raise

            return cast(_F, trace_sync)

        @wraps(func)
        async def trace_async(*args: Any, **kwargs: Any) -> Any:
            locals()[_CALL_ARGS] = _CallArgs.create(func, *args, **kwargs)
            try:
                return await func(*args, **kwargs)
            except Exception as error:
                cast(Any, error).exc_tb = _get_rich_traceback_internal(error)
                raise

        return cast(_F, trace_async)

    if not iscoroutinefunction(func):

        @wraps(func)
        def trace_sync(*args: Any, **kwargs: Any) -> Any:
            if en := runtime():
                locals()[_CALL_ARGS] = _CallArgs.create(func, *args, **kwargs)
            try:
                return func(*args, **kwargs)
            except Exception as error:
                if en:
                    cast(Any, error).exc_tb = _get_rich_traceback_internal(error)
                raise

        return cast(_F, trace_sync)

    @wraps(func)
    async def trace_async(*args: Any, **kwargs: Any) -> Any:
        if en := runtime():
            locals()[_CALL_ARGS] = _CallArgs.create(func, *args, **kwargs)
        try:
            return await func(*args, **kwargs)
        except Exception as error:
            if en:
                cast(Any, error).exc_tb = _get_rich_traceback_internal(error)
            raise

    return cast(_F, trace_async)


@overload
def yield_extended_frame_summaries(
    error: BaseException, /, *, extra: Callable[[FrameSummary, FrameType], _T]
) -> Iterator[_ExtFrameSummary[_T]]: ...
@overload
def yield_extended_frame_summaries(
    error: BaseException, /, *, extra: None = None
) -> Iterator[_ExtFrameSummary[None]]: ...
def yield_extended_frame_summaries(
    error: BaseException,
    /,
    *,
    extra: Callable[[FrameSummary, FrameType], _T] | None = None,
) -> Iterator[_ExtFrameSummary[Any]]:
    """Yield the extended frame summaries."""
    tb_exc = TracebackException.from_exception(error, capture_locals=True)
    _, _, traceback = exc_info()
    frames = yield_frames(traceback=traceback)
    for summary, frame in zip(tb_exc.stack, frames, strict=True):
        if extra is None:
            extra_use: _T | None = None
        else:
            extra_use: _T | None = extra(summary, frame)
        yield _ExtFrameSummary(
            filename=Path(summary.filename),
            module=frame.f_globals.get("__name__"),
            name=summary.name,
            qualname=frame.f_code.co_qualname,
            code_line=ensure_not_none(summary.line, desc="summary.line"),
            first_line_num=frame.f_code.co_firstlineno,
            line_num=ensure_not_none(summary.lineno, desc="summary.lineno"),
            end_line_num=ensure_not_none(summary.end_lineno, desc="summary.end_lineno"),
            col_num=summary.colno,
            end_col_num=summary.end_colno,
            locals=frame.f_locals,
            extra=extra_use,
        )


def yield_exceptions(error: BaseException, /) -> Iterator[BaseException]:
    """Yield the exceptions in a context chain."""
    curr: BaseException | None = error
    while curr is not None:
        yield curr
        curr = curr.__context__


def yield_frames(*, traceback: TracebackType | None = None) -> Iterator[FrameType]:
    """Yield the frames of a traceback."""
    while traceback is not None:
        yield traceback.tb_frame
        traceback = traceback.tb_next


def _format_exception(
    error: BaseException, /, *, detail: bool = False, depth: int = 0
) -> str:
    """Format an exception."""
    lines: list[str] = []
    cls = get_class_name(error)
    if detail:
        lines.extend([f"{cls}:", indent(str(error), _INDENT)])
    else:
        lines.append(cls)
    return indent("\n".join(lines), depth * _INDENT)


def _get_rich_traceback_internal(error: BaseException, /) -> _ExcTBInternal:
    """Get a rich traceback; for internal use only."""

    def extra(_: FrameSummary, frame: FrameType) -> _CallArgs | None:
        return frame.f_locals.get(_CALL_ARGS)

    raw = list(yield_extended_frame_summaries(error, extra=extra))
    return _ExcTBInternal(raw=raw, frames=_merge_frames(raw), error=error)


def _merge_frames(
    frames: Iterable[_ExtFrameSummaryCAOpt], /
) -> list[_ExtFrameSummaryCA]:
    """Merge a set of frames."""
    rev = list(frames)[::-1]
    values: list[_ExtFrameSummaryCA] = []

    def get_solution(
        curr: _ExtFrameSummaryCAOpt, rev: list[_ExtFrameSummaryCAOpt], /
    ) -> _ExtFrameSummaryCA:
        while True:
            next_ = rev.pop(0)
            if has_extra(next_) and is_match(curr, next_):
                return next_

    def has_extra(frame: _ExtFrameSummaryCAOpt, /) -> TypeGuard[_ExtFrameSummaryCA]:
        return frame.extra is not None

    def has_match(
        curr: _ExtFrameSummaryCAOpt, rev: list[_ExtFrameSummaryCAOpt], /
    ) -> bool:
        next_, *_ = filter(has_extra, rev)
        return is_match(curr, next_)

    def is_match(curr: _ExtFrameSummaryCAOpt, next_: _ExtFrameSummaryCA, /) -> bool:
        return (curr.name == next_.extra.func.__name__) and (
            (curr.module is None) or (curr.module == next_.extra.func.__module__)
        )

    while len(rev) >= 1:
        curr = rev.pop(0)
        if not has_match(curr, rev):
            continue
        next_ = get_solution(curr, rev)
        new = cast(_ExtFrameSummaryCA, replace(curr, extra=next_.extra))
        values.append(new)
    return values[::-1]


def _yield_header_lines() -> Iterator[str]:
    """Yield the header lines."""
    yield f"Date/time | {serialize_zoned_datetime(get_now(time_zone='local'))}"
    yield f"User      | {getuser()}"
    yield f"Host      | {gethostname()}"
    if (version := get_version()) is not None:
        yield f"Version   | {version}"
    yield ""


__all__ = [
    "ExcChainTB",
    "ExcGroupTB",
    "ExcInfo",
    "ExcTB",
    "OptExcInfo",
    "RichTracebackFormatter",
    "get_rich_traceback",
    "trace",
    "yield_exceptions",
    "yield_extended_frame_summaries",
    "yield_frames",
]
