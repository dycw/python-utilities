from __future__ import annotations

import re
import sys
from asyncio import run
from collections.abc import Callable
from dataclasses import dataclass
from functools import partial
from getpass import getuser
from itertools import repeat
from pathlib import Path
from socket import gethostname
from traceback import TracebackException
from typing import TYPE_CHECKING, override

from utilities.datetime import get_datetime, get_now, serialize_compact
from utilities.errors import repr_error
from utilities.iterables import OneEmptyError, one
from utilities.pathlib import get_path
from utilities.reprlib import (
    RICH_EXPAND_ALL,
    RICH_INDENT_SIZE,
    RICH_MAX_DEPTH,
    RICH_MAX_LENGTH,
    RICH_MAX_STRING,
    RICH_MAX_WIDTH,
    yield_mapping_repr,
)
from utilities.version import get_version
from utilities.whenever import serialize_duration

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator, Sequence
    from traceback import FrameSummary
    from types import TracebackType

    from utilities.types import MaybeCallableDateTime, MaybeCallablePathLike, PathLike
    from utilities.version import MaybeCallableVersionLike


##


_START = get_now()


def format_exception_stack(
    error: BaseException,
    /,
    *,
    header: bool = False,
    start: MaybeCallableDateTime | None = _START,
    version: MaybeCallableVersionLike | None = None,
    capture_locals: bool = False,
    max_width: int = RICH_MAX_WIDTH,
    indent_size: int = RICH_INDENT_SIZE,
    max_length: int | None = RICH_MAX_LENGTH,
    max_string: int | None = RICH_MAX_STRING,
    max_depth: int | None = RICH_MAX_DEPTH,
    expand_all: bool = RICH_EXPAND_ALL,
) -> str:
    """Format an exception stack."""
    lines: Sequence[str] = []
    if header:
        lines.extend(_yield_header_lines(start=start, version=version))
    lines.extend(
        _yield_formatted_frame_summary(
            error,
            capture_locals=capture_locals,
            max_width=max_width,
            indent_size=indent_size,
            max_length=max_length,
            max_string=max_string,
            max_depth=max_depth,
            expand_all=expand_all,
        )
    )
    return "\n".join(lines)


def _yield_header_lines(
    *,
    start: MaybeCallableDateTime | None = _START,
    version: MaybeCallableVersionLike | None = None,
) -> Iterator[str]:
    """Yield the header lines."""
    from utilities.tzlocal import get_local_time_zone, get_now_local
    from utilities.whenever import serialize_zoned_datetime

    now = get_now_local()
    start_use = get_datetime(datetime=start)
    start_use = (
        None if start_use is None else start_use.astimezone(get_local_time_zone())
    )
    yield f"Date/time | {serialize_zoned_datetime(now)}"
    start_str = "" if start_use is None else serialize_zoned_datetime(start_use)
    yield f"Started   | {start_str}"
    duration = None if start_use is None else (now - start_use)
    duration_str = "" if duration is None else serialize_duration(duration)
    yield f"Duration  | {duration_str}"
    yield f"User      | {getuser()}"
    yield f"Host      | {gethostname()}"
    version_use = "" if version is None else get_version(version=version)
    yield f"Version   | {version_use}"
    yield ""


def _yield_formatted_frame_summary(
    error: BaseException,
    /,
    *,
    capture_locals: bool = False,
    max_width: int = RICH_MAX_WIDTH,
    indent_size: int = RICH_INDENT_SIZE,
    max_length: int | None = RICH_MAX_LENGTH,
    max_string: int | None = RICH_MAX_STRING,
    max_depth: int | None = RICH_MAX_DEPTH,
    expand_all: bool = RICH_EXPAND_ALL,
) -> Iterator[str]:
    """Yield the formatted frame summary lines."""
    stack = TracebackException.from_exception(
        error, capture_locals=capture_locals
    ).stack
    n = len(stack)
    for i, frame in enumerate(stack, start=1):
        num = f"{i}/{n}"
        first, *rest = _yield_frame_summary_lines(
            frame,
            max_width=max_width,
            indent_size=indent_size,
            max_length=max_length,
            max_string=max_string,
            max_depth=max_depth,
            expand_all=expand_all,
        )
        yield f"{num} | {first}"
        blank = "".join(repeat(" ", len(num)))
        for rest_i in rest:
            yield f"{blank} | {rest_i}"
    yield repr_error(error)


def _yield_frame_summary_lines(
    frame: FrameSummary,
    /,
    *,
    max_width: int = RICH_MAX_WIDTH,
    indent_size: int = RICH_INDENT_SIZE,
    max_length: int | None = RICH_MAX_LENGTH,
    max_string: int | None = RICH_MAX_STRING,
    max_depth: int | None = RICH_MAX_DEPTH,
    expand_all: bool = RICH_EXPAND_ALL,
) -> Iterator[str]:
    module = _path_to_dots(frame.filename)
    yield f"{module}:{frame.lineno} | {frame.name} | {frame.line}"
    if frame.locals is not None:
        yield from yield_mapping_repr(
            frame.locals,
            _max_width=max_width,
            _indent_size=indent_size,
            _max_length=max_length,
            _max_string=max_string,
            _max_depth=max_depth,
            _expand_all=expand_all,
        )


def _path_to_dots(path: PathLike, /) -> str:
    new_path: Path | None = None
    for pattern in [
        "site-packages",
        ".venv",  # after site-packages
        "src",
        r"python\d+\.\d+",
    ]:
        if (new_path := _trim_path(path, pattern)) is not None:
            break
    path_use = Path(path) if new_path is None else new_path
    return ".".join(path_use.with_suffix("").parts)


def _trim_path(path: PathLike, pattern: str, /) -> Path | None:
    parts = Path(path).parts
    compiled = re.compile(f"^{pattern}$")
    try:
        i = one(i for i, p in enumerate(parts) if compiled.search(p))
    except OneEmptyError:
        return None
    return Path(*parts[i + 1 :])


##


def make_except_hook(
    *,
    start: MaybeCallableDateTime | None = _START,
    version: MaybeCallableVersionLike | None = None,
    path: MaybeCallablePathLike | None = None,
    max_width: int = RICH_MAX_WIDTH,
    indent_size: int = RICH_INDENT_SIZE,
    max_length: int | None = RICH_MAX_LENGTH,
    max_string: int | None = RICH_MAX_STRING,
    max_depth: int | None = RICH_MAX_DEPTH,
    expand_all: bool = RICH_EXPAND_ALL,
    slack_url: str | None = None,
) -> Callable[
    [type[BaseException] | None, BaseException | None, TracebackType | None], None
]:
    """Exception hook to log the traceback."""
    return partial(
        _make_except_hook_inner,
        start=start,
        version=version,
        path=path,
        max_width=max_width,
        indent_size=indent_size,
        max_length=max_length,
        max_string=max_string,
        max_depth=max_depth,
        expand_all=expand_all,
        slack_url=slack_url,
    )


def _make_except_hook_inner(
    exc_type: type[BaseException] | None,
    exc_val: BaseException | None,
    traceback: TracebackType | None,
    /,
    *,
    start: MaybeCallableDateTime | None = _START,
    version: MaybeCallableVersionLike | None = None,
    path: MaybeCallablePathLike | None = None,
    max_width: int = RICH_MAX_WIDTH,
    indent_size: int = RICH_INDENT_SIZE,
    max_length: int | None = RICH_MAX_LENGTH,
    max_string: int | None = RICH_MAX_STRING,
    max_depth: int | None = RICH_MAX_DEPTH,
    expand_all: bool = RICH_EXPAND_ALL,
    slack_url: str | None = None,
) -> None:
    """Exception hook to log the traceback."""
    _ = (exc_type, traceback)
    if exc_val is None:
        raise MakeExceptHookError
    slim = format_exception_stack(exc_val, header=True, start=start, version=version)
    _ = sys.stderr.write(f"{slim}\n")  # don't 'from sys import stderr'
    if path is not None:
        from utilities.atomicwrites import writer
        from utilities.tzlocal import get_now_local

        path = (
            get_path(path=path)
            .joinpath(serialize_compact(get_now_local()))
            .with_suffix(".txt")
        )
        full = format_exception_stack(
            exc_val,
            header=True,
            start=start,
            version=version,
            capture_locals=True,
            max_width=max_width,
            indent_size=indent_size,
            max_length=max_length,
            max_string=max_string,
            max_depth=max_depth,
            expand_all=expand_all,
        )
        with writer(path, overwrite=True) as temp:
            _ = temp.write_text(full)
    if slack_url is not None:  # pragma: no cover
        from utilities.slack_sdk import send_to_slack

        send = f"```{slim}```"
        run(send_to_slack(slack_url, send))


@dataclass(kw_only=True, slots=True)
class MakeExceptHookError(Exception):
    @override
    def __str__(self) -> str:
        return "No exception to log"


__all__ = ["format_exception_stack", "make_except_hook"]
