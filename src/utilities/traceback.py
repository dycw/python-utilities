from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import dataclass, field, replace
from functools import partial, wraps
from inspect import iscoroutinefunction, signature
from logging import NOTSET, Handler, LogRecord
from pathlib import Path
from sys import exc_info
from traceback import FrameSummary, TracebackException, print_exception
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

from utilities.atomicwrites import writer
from utilities.datetime import get_now
from utilities.errors import ImpossibleCaseError
from utilities.functions import (
    ensure_not_none,
    get_class_name,
    get_func_name,
    get_func_qualname,
)
from utilities.iterables import one
from utilities.logging import get_default_logging_path
from utilities.pathlib import resolve_path
from utilities.rich import yield_pretty_repr_args_and_kwargs
from utilities.text import ensure_str

if TYPE_CHECKING:
    from collections.abc import Iterator
    from types import FrameType

    from utilities.types import PathLikeOrCallable


_F = TypeVar("_F", bound=Callable[..., Any])
_T = TypeVar("_T")
_TExc = TypeVar("_TExc", bound=BaseException)
_CALL_ARGS = "_CALL_ARGS"
ExcInfo: TypeAlias = tuple[type[BaseException], BaseException, TracebackType]
OptExcInfo: TypeAlias = ExcInfo | tuple[None, None, None]


def _get_default_logging_path() -> Path:
    """Get the logging default path."""
    return get_default_logging_path().joinpath("errors")


class TracebackHandler(Handler):
    """Handler for emitting tracebacks to individual files."""

    @override
    def __init__(
        self,
        *,
        level: int = NOTSET,
        path: PathLikeOrCallable | None = _get_default_logging_path,
        max_width: int = 80,
        indent_size: int = 4,
        max_length: int | None = None,
        max_string: int | None = None,
        max_depth: int | None = None,
        expand_all: bool = False,
    ) -> None:
        super().__init__(level=level)
        self._path = path
        self._max_width = max_width
        self._indent_size = indent_size
        self._max_length = max_length
        self._max_string = max_string
        self._max_depth = max_depth
        self._expand_all = expand_all

    @override
    def emit(self, record: LogRecord) -> None:
        if record.exc_info is None:
            return
        _, exc_value, _ = record.exc_info
        if exc_value is None:
            return
        assembled = assemble_exception_paths(exc_value)
        path = (
            resolve_path(path=self._path)
            .joinpath(get_now(time_zone="local").strftime("%Y-%m-%dT%H-%M-%S"))
            .with_suffix(".txt")
        )
        try:
            from rich.pretty import pretty_repr
        except ImportError:  # pragma: no cover
            repr_use = repr(assembled)
        else:
            repr_use = pretty_repr(
                assembled,
                max_width=self._max_width,
                indent_size=self._indent_size,
                max_length=self._max_length,
                max_string=self._max_string,
                max_depth=self._max_depth,
                expand_all=self._expand_all,
            )

        with writer(path) as temp, temp.open(mode="w") as fh:
            _ = fh.write(repr_use)


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
            lines.extend(yield_pretty_repr_args_and_kwargs(*args, **kwargs))
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
class _ExceptionPathInternal:
    """An (internal) exception path."""

    raw: list[_ExtFrameSummaryCAOpt] = field(default_factory=list)
    frames: list[_ExtFrameSummaryCA] = field(default_factory=list)
    error: BaseException


@runtime_checkable
class _HasExceptionPath(Protocol):
    @property
    def exc_path(self) -> _ExceptionPathInternal: ...  # pragma: no cover


@dataclass(kw_only=True, slots=True)
class ExcChain(Generic[_TExc]):
    errors: list[ExcGroup[_TExc] | ExcPath[_TExc] | BaseException] = field(
        default_factory=list
    )

    def __iter__(self) -> Iterator[ExcGroup[_TExc] | ExcPath[_TExc] | BaseException]:
        yield from self.errors

    def __len__(self) -> int:
        return len(self.errors)


@dataclass(kw_only=True, slots=True)
class ExcGroup(Generic[_TExc]):
    path: ExcPath[_TExc]
    errors: list[ExcGroup[_TExc] | ExcPath[_TExc] | BaseException] = field(
        default_factory=list
    )


@dataclass(kw_only=True, slots=True)
class ExcPath(Generic[_TExc]):
    frames: list[_Frame] = field(default_factory=list)
    error: _TExc

    def __iter__(self) -> Iterator[_Frame]:
        yield from self.frames

    def __len__(self) -> int:
        return len(self.frames)


@dataclass(kw_only=True, slots=True)
class _Frame:
    module: str | None = None
    name: str
    code_line: str
    line_num: int
    args: tuple[Any, ...] = field(default_factory=tuple)
    kwargs: dict[str, Any] = field(default_factory=dict)
    locals: dict[str, Any] = field(default_factory=dict)


def assemble_exception_paths(
    error: _TExc, /
) -> ExcChain[_TExc] | ExcGroup[_TExc] | ExcPath[_TExc] | BaseException:
    """Assemble a set of extended tracebacks."""
    match list(yield_exceptions(error)):
        case []:  # pragma: no cover
            raise ImpossibleCaseError(case=[f"{error}"])
        case [err]:
            err = cast(_TExc, err)
            return _assemble_exception_paths_no_chain(err)
        case errors:
            errs = cast(list[_TExc], errors[::-1])
            return ExcChain(
                errors=[_assemble_exception_paths_no_chain(e) for e in errs]
            )


def _assemble_exception_paths_no_chain(
    error: _TExc, /
) -> ExcPath[_TExc] | ExcGroup[_TExc] | BaseException:
    if not isinstance(error, ExceptionGroup):
        return _assemble_exception_paths_no_chain_no_group(error)
    path = cast(ExcPath[_TExc], _assemble_exception_paths_no_chain_no_group(error))
    errors = cast(list[_TExc], error.exceptions)
    errors = list(map(_assemble_exception_paths_no_chain, errors))
    return ExcGroup(path=path, errors=errors)


def _assemble_exception_paths_no_chain_no_group(
    error: _TExc, /
) -> ExcPath[_TExc] | BaseException:
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
            for f in error.exc_path.frames
        ]
        return ExcPath(frames=frames, error=cast(_TExc, error))
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
                    cast(Any, error).exc_path = _get_call_frame_summaries(error)
                    raise

            return cast(_F, trace_sync)

        @wraps(func)
        async def trace_async(*args: Any, **kwargs: Any) -> Any:
            locals()[_CALL_ARGS] = _CallArgs.create(func, *args, **kwargs)
            try:
                return await func(*args, **kwargs)
            except Exception as error:
                cast(Any, error).exc_path = _get_call_frame_summaries(error)
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
                    cast(Any, error).exc_path = _get_call_frame_summaries(error)
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
                cast(Any, error).exc_path = _get_call_frame_summaries(error)
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


def _get_call_frame_summaries(error: BaseException, /) -> _ExceptionPathInternal:
    """Get an extended traceback."""

    def extra(_: FrameSummary, frame: FrameType) -> _CallArgs | None:
        return frame.f_locals.get(_CALL_ARGS)

    raw = list(yield_extended_frame_summaries(error, extra=extra))
    return _ExceptionPathInternal(raw=raw, frames=_merge_frames(raw), error=error)


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


__all__ = [
    "ExcChain",
    "ExcGroup",
    "ExcInfo",
    "ExcPath",
    "OptExcInfo",
    "TracebackHandler",
    "assemble_exception_paths",
    "trace",
    "yield_exceptions",
    "yield_extended_frame_summaries",
    "yield_frames",
]
