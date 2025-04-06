from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import dataclass, field, replace
from functools import wraps
from getpass import getuser
from inspect import iscoroutinefunction, signature
from logging import Formatter, Handler, LogRecord
from pathlib import Path
from socket import gethostname
from sys import exc_info
from textwrap import indent
from traceback import FrameSummary, TracebackException, format_exception
from typing import (
    TYPE_CHECKING,
    Any,
    Generic,
    Protocol,
    Self,
    TypeGuard,
    TypeVar,
    assert_never,
    cast,
    overload,
    override,
    runtime_checkable,
)

from utilities.datetime import get_now_local
from utilities.errors import ImpossibleCaseError
from utilities.functions import (
    ensure_not_none,
    ensure_str,
    get_class_name,
    get_func_name,
    get_func_qualname,
)
from utilities.iterables import always_iterable, one
from utilities.reprlib import (
    RICH_EXPAND_ALL,
    RICH_INDENT_SIZE,
    RICH_MAX_DEPTH,
    RICH_MAX_LENGTH,
    RICH_MAX_STRING,
    RICH_MAX_WIDTH,
)
from utilities.rich import yield_call_args_repr, yield_mapping_repr
from utilities.types import TBaseException, TCallable
from utilities.version import get_version
from utilities.whenever import serialize_zoned_datetime

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Iterator
    from logging import _FormatStyle
    from types import FrameType, TracebackType

    from utilities.types import Coroutine1, StrMapping
    from utilities.version import MaybeCallableVersionLike


_T = TypeVar("_T")
_CALL_ARGS = "_CALL_ARGS"
_INDENT = 4 * " "


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
        version: MaybeCallableVersionLike | None = None,
        max_width: int = RICH_MAX_WIDTH,
        indent_size: int = RICH_INDENT_SIZE,
        max_length: int | None = RICH_MAX_LENGTH,
        max_string: int | None = RICH_MAX_STRING,
        max_depth: int | None = RICH_MAX_DEPTH,
        expand_all: bool = RICH_EXPAND_ALL,
        detail: bool = False,
        post: Callable[[str], str] | None = None,
    ) -> None:
        super().__init__(fmt, datefmt, style, validate, defaults=defaults)
        self._version = version
        self._max_width = max_width
        self._indent_size = indent_size
        self._max_length = max_length
        self._max_string = max_string
        self._max_depth = max_depth
        self._expand_all = expand_all
        self._detail = detail
        self._post = post

    @override
    def format(self, record: LogRecord) -> str:
        """Format the record."""
        if record.exc_info is None:
            return f"ERROR: record.exc_info is None\n{record=}"
        _, exc_value, _ = record.exc_info
        if exc_value is None:
            return f"ERROR: record.exc_info[1] is None\n{record=}"  # pragma: no cover
        exc_value = ensure_not_none(exc_value, desc="exc_value")
        error = get_rich_traceback(
            exc_value,
            version=self._version,
            max_width=self._max_width,
            indent_size=self._indent_size,
            max_length=self._max_length,
            max_string=self._max_string,
            max_depth=self._max_depth,
            expand_all=self._expand_all,
        )
        match error:
            case ExcChainTB() | ExcGroupTB() | ExcTB():
                text = error.format(header=True, detail=self._detail)
            case BaseException():
                text = "\n".join(format_exception(error))
            case _ as never:
                assert_never(never)
        if self._post is not None:
            text = self._post(text)
        return text

    @classmethod
    def create_and_set(
        cls,
        handler: Handler,
        /,
        *,
        fmt: str | None = None,
        datefmt: str | None = None,
        style: _FormatStyle = "%",
        validate: bool = True,
        defaults: StrMapping | None = None,
        version: MaybeCallableVersionLike | None = None,
        max_width: int = RICH_MAX_WIDTH,
        indent_size: int = RICH_INDENT_SIZE,
        max_length: int | None = RICH_MAX_LENGTH,
        max_string: int | None = RICH_MAX_STRING,
        max_depth: int | None = RICH_MAX_DEPTH,
        expand_all: bool = RICH_EXPAND_ALL,
        detail: bool = False,
        post: Callable[[str], str] | None = None,
    ) -> Self:
        """Create an instance and set it on a handler."""
        formatter = cls(
            fmt,
            datefmt,
            style,
            validate,
            defaults=defaults,
            version=version,
            max_width=max_width,
            indent_size=indent_size,
            max_length=max_length,
            max_string=max_string,
            max_depth=max_depth,
            expand_all=expand_all,
            detail=detail,
            post=post,
        )
        handler.addFilter(lambda r: r.exc_info is not None)
        handler.setFormatter(formatter)
        return formatter


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


type _ExtFrameSummaryCAOpt = _ExtFrameSummary[_CallArgs | None]
type _ExtFrameSummaryCA = _ExtFrameSummary[_CallArgs]


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
class ExcChainTB(Generic[TBaseException]):
    """A rich traceback for an exception chain."""

    errors: list[
        ExcGroupTB[TBaseException] | ExcTB[TBaseException] | TBaseException
    ] = field(default_factory=list)
    version: MaybeCallableVersionLike | None = field(default=None, repr=False)
    max_width: int = RICH_MAX_WIDTH
    indent_size: int = RICH_INDENT_SIZE
    max_length: int | None = RICH_MAX_LENGTH
    max_string: int | None = RICH_MAX_STRING
    max_depth: int | None = RICH_MAX_DEPTH
    expand_all: bool = RICH_EXPAND_ALL

    def __getitem__(
        self, i: int, /
    ) -> ExcGroupTB[TBaseException] | ExcTB[TBaseException] | TBaseException:
        return self.errors[i]

    def __iter__(
        self,
    ) -> Iterator[ExcGroupTB[TBaseException] | ExcTB[TBaseException] | TBaseException]:
        yield from self.errors

    def __len__(self) -> int:
        return len(self.errors)

    @override
    def __repr__(self) -> str:
        return self.format(header=True, detail=True)

    def format(self, *, header: bool = False, detail: bool = False) -> str:
        """Format the traceback."""
        lines: list[str] = []
        if header:  # pragma: no cover
            lines.extend(_yield_header_lines(version=self.version))
        total = len(self.errors)
        for i, errors in enumerate(self.errors, start=1):
            lines.append(f"Exception chain {i}/{total}:")
            match errors:
                case ExcGroupTB() | ExcTB():
                    lines.append(errors.format(header=False, detail=detail, depth=1))
                case BaseException():  # pragma: no cover
                    lines.append(_format_exception(errors, depth=1))
                case _ as never:
                    assert_never(never)
            lines.append("")
        return "\n".join(lines)


@dataclass(kw_only=True, slots=True)
class ExcGroupTB(Generic[TBaseException]):
    """A rich traceback for an exception group."""

    exc_group: ExcTB[ExceptionGroup[Any]] | ExceptionGroup[Any]
    errors: list[
        ExcGroupTB[TBaseException] | ExcTB[TBaseException] | TBaseException
    ] = field(default_factory=list)
    version: MaybeCallableVersionLike | None = field(default=None, repr=False)
    max_width: int = RICH_MAX_WIDTH
    indent_size: int = RICH_INDENT_SIZE
    max_length: int | None = RICH_MAX_LENGTH
    max_string: int | None = RICH_MAX_STRING
    max_depth: int | None = RICH_MAX_DEPTH
    expand_all: bool = RICH_EXPAND_ALL

    @override
    def __repr__(self) -> str:
        return self.format(header=True, detail=True)  # skipif-ci

    def format(
        self, *, header: bool = False, detail: bool = False, depth: int = 0
    ) -> str:
        """Format the traceback."""
        lines: list[str] = []  # skipif-ci
        if header:  # pragma: no cover
            lines.extend(_yield_header_lines(version=self.version))
        lines.append("Exception group:")  # skipif-ci
        match self.exc_group:  # skipif-ci
            case ExcTB() as exc_tb:
                lines.append(exc_tb.format(header=False, detail=detail, depth=1))
            case ExceptionGroup() as exc_group:  # pragma: no cover
                lines.append(_format_exception(exc_group, depth=1))
            case _ as never:
                assert_never(never)
        lines.append("")  # skipif-ci
        total = len(self.errors)  # skipif-ci
        for i, errors in enumerate(self.errors, start=1):  # skipif-ci
            lines.append(indent(f"Exception group error {i}/{total}:", _INDENT))
            match errors:
                case ExcGroupTB() | ExcTB():  # pragma: no cover
                    lines.append(errors.format(header=False, detail=detail, depth=2))
                case BaseException():  # pragma: no cover
                    lines.append(_format_exception(errors, depth=2))
                case _ as never:
                    assert_never(never)
            lines.append("")
        return indent("\n".join(lines), depth * _INDENT)  # skipif-ci


@dataclass(kw_only=True, slots=True)
class ExcTB(Generic[TBaseException]):
    """A rich traceback for a single exception."""

    frames: list[_Frame] = field(default_factory=list)
    error: TBaseException
    version: MaybeCallableVersionLike | None = field(default=None, repr=False)
    max_width: int = RICH_MAX_WIDTH
    indent_size: int = RICH_INDENT_SIZE
    max_length: int | None = RICH_MAX_LENGTH
    max_string: int | None = RICH_MAX_STRING
    max_depth: int | None = RICH_MAX_DEPTH
    expand_all: bool = RICH_EXPAND_ALL

    def __getitem__(self, i: int, /) -> _Frame:
        return self.frames[i]

    def __iter__(self) -> Iterator[_Frame]:
        yield from self.frames

    def __len__(self) -> int:
        return len(self.frames)

    @override
    def __repr__(self) -> str:
        return self.format(header=True, detail=True)

    def format(
        self, *, header: bool = False, detail: bool = False, depth: int = 0
    ) -> str:
        """Format the traceback."""
        total = len(self)
        lines: list[str] = []
        if header:  # pragma: no cover
            lines.extend(_yield_header_lines(version=self.version))
        for i, frame in enumerate(self.frames):
            is_head = i < total - 1
            lines.append(
                frame.format(
                    index=i,
                    total=total,
                    detail=detail,
                    error=None if is_head else self.error,
                )
            )
            if detail and is_head:
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
    max_width: int = RICH_MAX_WIDTH
    indent_size: int = RICH_INDENT_SIZE
    max_length: int | None = RICH_MAX_LENGTH
    max_string: int | None = RICH_MAX_STRING
    max_depth: int | None = RICH_MAX_DEPTH
    expand_all: bool = RICH_EXPAND_ALL

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
    ) -> str:
        """Format the traceback."""
        lines: list[str] = [f"Frame {index + 1}/{total}: {self.name} ({self.module})"]
        if detail:
            lines.append(indent("Inputs:", _INDENT))
            lines.extend(
                indent(line, 2 * _INDENT)
                for line in yield_call_args_repr(
                    *self.args,
                    _max_width=self.max_width,
                    _indent_size=self.indent_size,
                    _max_length=self.max_length,
                    _max_string=self.max_string,
                    _max_depth=self.max_depth,
                    _expand_all=self.expand_all,
                    **self.kwargs,
                )
            )
            lines.append(indent("Locals:", _INDENT))
            lines.extend(
                indent(line, 2 * _INDENT)
                for line in yield_mapping_repr(
                    _max_width=self.max_width,
                    _indent_size=self.indent_size,
                    _max_length=self.max_length,
                    _max_string=self.max_string,
                    _max_depth=self.max_depth,
                    _expand_all=self.expand_all,
                    **self.locals,
                )
            )
            lines.extend([
                indent(f"Line {self.line_num}:", _INDENT),
                indent(self.code_line, 2 * _INDENT),
            ])
        if error is not None:
            lines.extend([
                indent("Raised:", _INDENT),
                _format_exception(error, depth=2),
            ])
        return indent("\n".join(lines), depth * _INDENT)


def get_rich_traceback(
    error: TBaseException,
    /,
    *,
    version: MaybeCallableVersionLike | None = None,
    max_width: int = RICH_MAX_WIDTH,
    indent_size: int = RICH_INDENT_SIZE,
    max_length: int | None = RICH_MAX_LENGTH,
    max_string: int | None = RICH_MAX_STRING,
    max_depth: int | None = RICH_MAX_DEPTH,
    expand_all: bool = RICH_EXPAND_ALL,
) -> (
    ExcChainTB[TBaseException]
    | ExcGroupTB[TBaseException]
    | ExcTB[TBaseException]
    | TBaseException
):
    """Get a rich traceback."""
    match list(yield_exceptions(error)):
        case []:  # pragma: no cover
            raise ImpossibleCaseError(case=[f"{error}"])
        case [err]:
            err_recast = cast("TBaseException", err)
            return _get_rich_traceback_non_chain(
                err_recast,
                version=version,
                max_width=max_width,
                indent_size=indent_size,
                max_length=max_length,
                max_string=max_string,
                max_depth=max_depth,
                expand_all=expand_all,
            )
        case errs:
            errs_recast = cast("list[TBaseException]", errs)
            return ExcChainTB(
                errors=[
                    _get_rich_traceback_non_chain(
                        e,
                        version=version,
                        max_width=max_width,
                        indent_size=indent_size,
                        max_length=max_length,
                        max_string=max_string,
                        max_depth=max_depth,
                        expand_all=expand_all,
                    )
                    for e in errs_recast
                ],
                version=version,
                max_width=max_width,
                indent_size=indent_size,
                max_length=max_length,
                max_string=max_string,
                max_depth=max_depth,
                expand_all=expand_all,
            )


def _get_rich_traceback_non_chain(
    error: ExceptionGroup[Any] | TBaseException,
    /,
    *,
    version: MaybeCallableVersionLike | None = None,
    max_width: int = RICH_MAX_WIDTH,
    indent_size: int = RICH_INDENT_SIZE,
    max_length: int | None = RICH_MAX_LENGTH,
    max_string: int | None = RICH_MAX_STRING,
    max_depth: int | None = RICH_MAX_DEPTH,
    expand_all: bool = RICH_EXPAND_ALL,
) -> ExcGroupTB[TBaseException] | ExcTB[TBaseException] | TBaseException:
    """Get a rich traceback, for a non-chained error."""
    match error:
        case ExceptionGroup() as exc_group:  # skipif-ci
            exc_group_or_exc_tb = _get_rich_traceback_base_one(
                exc_group,
                max_width=max_width,
                indent_size=indent_size,
                max_length=max_length,
                max_string=max_string,
                max_depth=max_depth,
                expand_all=expand_all,
            )
            errors = [
                _get_rich_traceback_non_chain(
                    e,
                    version=version,
                    max_width=max_width,
                    indent_size=indent_size,
                    max_length=max_length,
                    max_string=max_string,
                    max_depth=max_depth,
                    expand_all=expand_all,
                )
                for e in always_iterable(exc_group.exceptions)
            ]
            return ExcGroupTB(
                exc_group=exc_group_or_exc_tb,
                errors=errors,
                version=version,
                max_width=max_width,
                indent_size=indent_size,
                max_length=max_length,
                max_string=max_string,
                max_depth=max_depth,
                expand_all=expand_all,
            )
        case BaseException() as base_exc:
            return _get_rich_traceback_base_one(
                base_exc,
                version=version,
                max_width=max_width,
                indent_size=indent_size,
                max_length=max_length,
                max_string=max_string,
                max_depth=max_depth,
                expand_all=expand_all,
            )
        case _ as never:
            assert_never(never)


def _get_rich_traceback_base_one(
    error: TBaseException,
    /,
    *,
    version: MaybeCallableVersionLike | None = None,
    max_width: int = RICH_MAX_WIDTH,
    indent_size: int = RICH_INDENT_SIZE,
    max_length: int | None = RICH_MAX_LENGTH,
    max_string: int | None = RICH_MAX_STRING,
    max_depth: int | None = RICH_MAX_DEPTH,
    expand_all: bool = RICH_EXPAND_ALL,
) -> ExcTB[TBaseException] | TBaseException:
    """Get a rich traceback, for a single exception."""
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
                max_width=max_width,
                indent_size=indent_size,
                max_length=max_length,
                max_string=max_string,
                max_depth=max_depth,
                expand_all=expand_all,
            )
            for f in error.exc_tb.frames
        ]
        return ExcTB(
            frames=frames,
            error=error,
            version=version,
            max_width=max_width,
            indent_size=indent_size,
            max_length=max_length,
            max_string=max_string,
            max_depth=max_depth,
            expand_all=expand_all,
        )
    return error


def trace(func: TCallable, /) -> TCallable:
    """Trace a function call."""
    match bool(iscoroutinefunction(func)):
        case False:
            func_typed = cast("Callable[..., Any]", func)

            @wraps(func)
            def trace_sync(*args: Any, **kwargs: Any) -> Any:
                locals()[_CALL_ARGS] = _CallArgs.create(func, *args, **kwargs)
                try:
                    return func_typed(*args, **kwargs)
                except Exception as error:
                    cast("Any", error).exc_tb = _get_rich_traceback_internal(error)
                    raise

            return cast("TCallable", trace_sync)
        case True:
            func_typed = cast("Callable[..., Coroutine1[Any]]", func)

            @wraps(func)
            async def trace_async(*args: Any, **kwargs: Any) -> Any:
                locals()[_CALL_ARGS] = _CallArgs.create(func, *args, **kwargs)
                try:  # skipif-ci
                    return await func_typed(*args, **kwargs)
                except Exception as error:  # skipif-ci
                    cast("Any", error).exc_tb = _get_rich_traceback_internal(error)
                    raise

            return cast("TCallable", trace_async)
        case _ as never:
            assert_never(never)


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


def _format_exception(error: BaseException, /, *, depth: int = 0) -> str:
    """Format an exception."""
    name = get_class_name(error, qual=True)
    line = f"{name}({error})"
    return indent(line, depth * _INDENT)


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
        new = cast("_ExtFrameSummaryCA", replace(curr, extra=next_.extra))
        values.append(new)
    return values[::-1]


def _yield_header_lines(
    *, version: MaybeCallableVersionLike | None = None
) -> Iterator[str]:
    """Yield the header lines."""
    yield f"Date/time | {serialize_zoned_datetime(get_now_local())}"
    yield f"User      | {getuser()}"
    yield f"Host      | {gethostname()}"
    version_use = "" if version is None else get_version(version=version)
    yield f"Version   | {version_use}"
    yield ""


__all__ = [
    "ExcChainTB",
    "ExcGroupTB",
    "ExcTB",
    "RichTracebackFormatter",
    "get_rich_traceback",
    "trace",
    "yield_exceptions",
    "yield_extended_frame_summaries",
    "yield_frames",
]
