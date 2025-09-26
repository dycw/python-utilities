from __future__ import annotations

import re
import sys
from dataclasses import dataclass
from functools import partial
from getpass import getuser
from itertools import repeat
from os import getpid
from pathlib import Path
from socket import gethostname
from sys import stderr
from traceback import TracebackException
from typing import TYPE_CHECKING, override

from utilities.atomicwrites import writer
from utilities.errors import repr_error
from utilities.iterables import OneEmptyError, one
from utilities.pathlib import module_path, to_path
from utilities.reprlib import (
    RICH_EXPAND_ALL,
    RICH_INDENT_SIZE,
    RICH_MAX_DEPTH,
    RICH_MAX_LENGTH,
    RICH_MAX_STRING,
    RICH_MAX_WIDTH,
    yield_mapping_repr,
)
from utilities.text import to_bool
from utilities.tzlocal import LOCAL_TIME_ZONE_NAME
from utilities.version import to_version
from utilities.whenever import (
    format_compact,
    get_now,
    get_now_local,
    to_zoned_date_time,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator
    from traceback import FrameSummary
    from types import TracebackType

    from utilities.types import (
        Delta,
        MaybeCallableBoolLike,
        MaybeCallablePathLike,
        MaybeCallableZonedDateTimeLike,
        PathLike,
    )
    from utilities.version import MaybeCallableVersionLike


##


def format_exception_stack(
    error: BaseException,
    /,
    *,
    header: bool = False,
    start: MaybeCallableZonedDateTimeLike = get_now,
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
    lines: list[str] = []
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
    start: MaybeCallableZonedDateTimeLike = get_now,
    version: MaybeCallableVersionLike | None = None,
) -> Iterator[str]:
    """Yield the header lines."""
    now = get_now_local()
    yield f"Date/time  | {format_compact(now)}"
    start_use = to_zoned_date_time(start).to_tz(LOCAL_TIME_ZONE_NAME)
    yield f"Started    | {format_compact(start_use)}"
    yield f"Duration   | {(now - start_use).format_iso()}"
    yield f"User       | {getuser()}"
    yield f"Host       | {gethostname()}"
    yield f"Process ID | {getpid()}"
    version_use = "" if version is None else to_version(version)
    yield f"Version    | {version_use}"
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
    return module_path(path_use)


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
    start: MaybeCallableZonedDateTimeLike = get_now,
    version: MaybeCallableVersionLike | None = None,
    path: MaybeCallablePathLike | None = None,
    path_max_age: Delta | None = None,
    max_width: int = RICH_MAX_WIDTH,
    indent_size: int = RICH_INDENT_SIZE,
    max_length: int | None = RICH_MAX_LENGTH,
    max_string: int | None = RICH_MAX_STRING,
    max_depth: int | None = RICH_MAX_DEPTH,
    expand_all: bool = RICH_EXPAND_ALL,
    slack_url: str | None = None,
    pudb: MaybeCallableBoolLike = False,
) -> Callable[
    [type[BaseException] | None, BaseException | None, TracebackType | None], None
]:
    """Exception hook to log the traceback."""
    return partial(
        _make_except_hook_inner,
        start=start,
        version=version,
        path=path,
        path_max_age=path_max_age,
        max_width=max_width,
        indent_size=indent_size,
        max_length=max_length,
        max_string=max_string,
        max_depth=max_depth,
        expand_all=expand_all,
        slack_url=slack_url,
        pudb=pudb,
    )


def _make_except_hook_inner(
    exc_type: type[BaseException] | None,
    exc_val: BaseException | None,
    traceback: TracebackType | None,
    /,
    *,
    start: MaybeCallableZonedDateTimeLike = get_now,
    version: MaybeCallableVersionLike | None = None,
    path: MaybeCallablePathLike | None = None,
    path_max_age: Delta | None = None,
    max_width: int = RICH_MAX_WIDTH,
    indent_size: int = RICH_INDENT_SIZE,
    max_length: int | None = RICH_MAX_LENGTH,
    max_string: int | None = RICH_MAX_STRING,
    max_depth: int | None = RICH_MAX_DEPTH,
    expand_all: bool = RICH_EXPAND_ALL,
    slack_url: str | None = None,
    pudb: MaybeCallableBoolLike = False,
) -> None:
    """Exception hook to log the traceback."""
    _ = (exc_type, traceback)
    if exc_val is None:
        raise MakeExceptHookError
    slim = format_exception_stack(exc_val, header=True, start=start, version=version)
    _ = sys.stderr.write(f"{slim}\n")  # don't 'from sys import stderr'
    if path is not None:
        path = to_path(path)
        path_log = path.joinpath(
            format_compact(get_now_local(), path=True)
        ).with_suffix(".txt")
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
        with writer(path_log, overwrite=True) as temp:
            _ = temp.write_text(full)
        if path_max_age is not None:
            _make_except_hook_purge(path, path_max_age)
    if slack_url is not None:  # pragma: no cover
        from utilities.slack_sdk import SendToSlackError, send_to_slack

        try:
            send_to_slack(slack_url, f"```{slim}```")
        except SendToSlackError as error:
            _ = stderr.write(f"{error}\n")
    if to_bool(pudb):  # pragma: no cover
        from pudb import post_mortem  # pyright: ignore[reportMissingImports]

        post_mortem(tb=traceback, e_type=exc_type, e_value=exc_val)


def _make_except_hook_purge(path: PathLike, max_age: Delta, /) -> None:
    threshold = get_now_local() - max_age
    paths: set[Path] = set()
    for p in Path(path).iterdir():
        if p.is_file():
            try:
                date_time = to_zoned_date_time(p.stem)
            except ValueError:
                pass
            else:
                if date_time <= threshold:
                    paths.add(p)
    for p in paths:
        p.unlink(missing_ok=True)


@dataclass(kw_only=True, slots=True)
class MakeExceptHookError(Exception):
    @override
    def __str__(self) -> str:
        return "No exception to log"


__all__ = ["format_exception_stack", "make_except_hook"]
