from __future__ import annotations

import re
import sys
from dataclasses import dataclass
from functools import partial
from os import getpid
from pathlib import Path
from sys import stderr
from textwrap import indent
from traceback import TracebackException
from typing import TYPE_CHECKING, Any, Literal, override

from utilities.constants import (
    HOSTNAME,
    LOCAL_TIME_ZONE_NAME,
    RICH_EXPAND_ALL,
    RICH_INDENT_SIZE,
    RICH_MAX_DEPTH,
    RICH_MAX_LENGTH,
    RICH_MAX_STRING,
    RICH_MAX_WIDTH,
    USER,
)
from utilities.core import (
    OneEmptyError,
    get_now,
    get_now_local,
    one,
    repr_error,
    repr_mapping,
    repr_table,
    write_text,
)
from utilities.pathlib import module_path, to_path
from utilities.text import to_bool
from utilities.version import to_version3
from utilities.whenever import format_compact, to_zoned_date_time

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
        TableLike,
    )
    from utilities.version import MaybeCallableVersion3Like


##


def format_exception_stack(
    error: BaseException,
    /,
    *,
    header: bool = False,
    start: MaybeCallableZonedDateTimeLike = get_now,
    version: MaybeCallableVersion3Like | None = None,
    capture_locals: bool = False,
    max_width: int = RICH_MAX_WIDTH,
    indent_size: int = RICH_INDENT_SIZE,
    max_length: int | None = RICH_MAX_LENGTH,
    max_string: int | None = RICH_MAX_STRING,
    max_depth: int | None = RICH_MAX_DEPTH,
    expand_all: bool = RICH_EXPAND_ALL,
) -> str:
    """Format an exception stack."""
    parts: list[str] = []
    if header:
        parts.append(_get_header(start=start, version=version))
    parts.append(
        _get_frame_summaries(
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
    return "\n".join(parts)


def _get_header(
    *,
    start: MaybeCallableZonedDateTimeLike = get_now,
    version: MaybeCallableVersion3Like | None = None,
) -> str:
    """Get the header."""
    items: list[tuple[str, Any]] = []
    start_use = to_zoned_date_time(start).to_tz(LOCAL_TIME_ZONE_NAME)
    now = get_now_local()
    items.append(("Date/time", format_compact(now)))
    items.extend([
        ("Started", format_compact(start_use)),
        ("Duration", (now - start_use).format_iso()),
        ("User", USER),
        ("Host", HOSTNAME),
        ("Process ID", getpid()),
    ])
    version_use = "" if version is None else to_version3(version)
    items.append(("Version", version_use))
    return repr_table(*items)


def _get_frame_summaries(
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
) -> str:
    """Get the frame summaries."""
    stack = TracebackException.from_exception(
        error, capture_locals=capture_locals
    ).stack
    items: list[tuple[int | Literal["E", ""], TableLike]] = []
    for i, frame in enumerate(stack, start=1):
        first, *rest = _yield_frame_summary_tables(
            frame,
            max_width=max_width,
            indent_size=indent_size,
            max_length=max_length,
            max_string=max_string,
            max_depth=max_depth,
            expand_all=expand_all,
        )
        items.append((i, first))
        items.extend([("", t) for t in rest])
    items.append(("E", repr_error(error)))
    return repr_table(
        *items,
        show_lines=True,
        max_width=max_width,
        indent_size=indent_size,
        max_length=max_length,
        max_string=max_string,
        max_depth=max_depth,
        expand_all=expand_all,
        header=[f"n={len(stack)}", ""],
    )


def _yield_frame_summary_tables(
    frame: FrameSummary,
    /,
    *,
    max_width: int = RICH_MAX_WIDTH,
    indent_size: int = RICH_INDENT_SIZE,
    max_length: int | None = RICH_MAX_LENGTH,
    max_string: int | None = RICH_MAX_STRING,
    max_depth: int | None = RICH_MAX_DEPTH,
    expand_all: bool = RICH_EXPAND_ALL,
) -> Iterator[TableLike]:
    module = _path_to_dots(frame.filename)
    parts: list[str] = [f"{module}:{frame.lineno}:{frame.name}"]
    if frame.line is not None:
        parts.append(indent(frame.line, 4 * " "))
    yield "\n".join(parts)
    if frame.locals is not None:
        yield repr_mapping(
            frame.locals,
            max_width=max_width,
            indent_size=indent_size,
            max_length=max_length,
            max_string=max_string,
            max_depth=max_depth,
            expand_all=expand_all,
            table=True,
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
    version: MaybeCallableVersion3Like | None = None,
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
    version: MaybeCallableVersion3Like | None = None,
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
        write_text(path_log, full, overwrite=True)
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
                ...
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
