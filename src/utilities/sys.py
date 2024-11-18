from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from functools import partial, wraps
from inspect import iscoroutinefunction, signature
from sys import exc_info, version_info
from typing import TYPE_CHECKING, Any, NoReturn, Self, TypeVar, cast, overload

from utilities.functions import get_func_name
from utilities.iterables import one
from utilities.traceback import _ExtFrameSummary, yield_extended_frame_summaries

if TYPE_CHECKING:
    from pathlib import Path

    from utilities.types import StrMapping

VERSION_MAJOR_MINOR = (version_info.major, version_info.minor)
_F = TypeVar("_F", bound=Callable[..., Any])
_MAX_WIDTH = 80
_INDENT_SIZE = 4


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
                call_args = _CallArgs.create(func, *args, **kwargs)
            except TypeError:
                return func(*args, **kwargs)
            try:
                return func(*args, **kwargs)
            except Exception as error:  # noqa: BLE001
                _trace_build_and_raise_trace_mixin(error, func, call_args)

        return trace_sync

    @wraps(func)
    async def log_call_async(*args: Any, **kwargs: Any) -> Any:
        try:
            call_args = _CallArgs.create(func, *args, **kwargs)
        except TypeError:
            return await func(*args, **kwargs)
        try:
            return await func(*args, **kwargs)
        except Exception as error:  # noqa: BLE001
            _trace_build_and_raise_trace_mixin(error, func, call_args)

    return cast(_F, log_call_async)


def _trace_build_and_raise_trace_mixin(
    error: Exception, func: Callable[..., Any], call_args: _CallArgs, /
) -> NoReturn:
    """Build and raise a TraceMixin exception."""
    _, _, traceback = exc_info()
    frames = list(yield_extended_frame_summaries(error, traceback=traceback))
    frame = one(f for f in frames if f.name == get_func_name(func))
    trace_frame = _RawTraceMixinFrame(call_args=call_args, ext_frame_summary=frame)
    cls = type(error)
    if isinstance(error, TraceMixin):
        bases = (cls,)
        raw_frames = [*error.raw_frames, trace_frame]
    else:
        bases = (cls, TraceMixin)
        raw_frames = [trace_frame]
    raise type(cls.__name__, bases, {"error": error, "raw_frames": raw_frames})(
        *error.args
    ) from None


@dataclass(kw_only=True, slots=True)
class _CallArgs:
    """A collection of call arguments."""

    func: Callable[..., Any]
    args: tuple[Any, ...] = field(default_factory=tuple)
    kwargs: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def create(cls, func: Callable[..., Any], *args: Any, **kwargs: Any) -> Self:
        """Make the initial trace data."""
        bound_args = signature(func).bind(*args, **kwargs)
        return cls(func=func, args=bound_args.args, kwargs=bound_args.kwargs)


@dataclass(kw_only=True, slots=False)  # no slots
class TraceMixin:
    """Mix-in for tracking an exception and its call stack."""

    exception: Exception
    raw_frames: list[_RawTraceMixinFrame] = field(default_factory=list)

    @property
    def frames(self) -> list[_TraceMixinFrame]:
        raw_frames = self.raw_frames
        return [
            _TraceMixinFrame(depth=i, max_depth=len(raw_frames), raw_frame=frame)
            for i, frame in enumerate(raw_frames[::-1], start=1)
        ]


@dataclass(kw_only=True, slots=True)
class _RawTraceMixinFrame:
    """A collection of call arguments and an extended frame summary."""

    call_args: _CallArgs
    ext_frame_summary: _ExtFrameSummary


@dataclass(kw_only=True, slots=True)
class _TraceMixinFrame:
    """A collection of call arguments and an extended frame summary."""

    depth: int
    max_depth: int
    raw_frame: _RawTraceMixinFrame

    @property
    def func(self) -> Callable[..., Any]:
        return self.raw_frame.call_args.func

    @property
    def args(self) -> tuple[Any, ...]:
        return self.raw_frame.call_args.args

    @property
    def kwargs(self) -> dict[str, Any]:
        return self.raw_frame.call_args.kwargs

    @property
    def filename(self) -> Path:
        return self.raw_frame.ext_frame_summary.filename

    @property
    def line_num(self) -> int | None:
        return self.raw_frame.ext_frame_summary.line_num

    @property
    def first_line_num(self) -> int:
        return self.raw_frame.ext_frame_summary.first_line_num

    @property
    def end_line_num(self) -> int | None:
        return self.raw_frame.ext_frame_summary.end_line_num

    @property
    def col_num(self) -> int | None:
        return self.raw_frame.ext_frame_summary.col_num

    @property
    def end_col_num(self) -> int | None:
        return self.raw_frame.ext_frame_summary.end_col_num

    @property
    def name(self) -> str:
        return self.raw_frame.ext_frame_summary.name

    @property
    def qualname(self) -> str:
        return self.raw_frame.ext_frame_summary.qualname

    @property
    def line(self) -> str | None:
        return self.raw_frame.ext_frame_summary.line

    @property
    def locals(self) -> StrMapping:
        return self.raw_frame.ext_frame_summary.locals


__all__ = ["VERSION_MAJOR_MINOR", "TraceMixin", "trace"]
