from __future__ import annotations

import datetime as dt
import re
from contextlib import contextmanager
from dataclasses import dataclass, field
from functools import cached_property
from itertools import product
from logging import (
    DEBUG,
    ERROR,
    NOTSET,
    FileHandler,
    Formatter,
    Handler,
    Logger,
    LogRecord,
    StreamHandler,
    basicConfig,
    getLevelNamesMapping,
    getLogger,
    setLogRecordFactory,
)
from logging.handlers import BaseRotatingHandler, TimedRotatingFileHandler
from pathlib import Path
from re import Pattern, search
from sys import stdout
from time import time
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    Literal,
    Self,
    assert_never,
    cast,
    override,
)

from utilities.atomicwrites import move_many, writer
from utilities.dataclasses import replace_non_sentinel
from utilities.datetime import (
    SECOND,
    get_now_local,
    maybe_sub_pct_y,
    parse_datetime_compact,
    round_datetime,
    serialize_compact,
)
from utilities.errors import ImpossibleCaseError
from utilities.git import get_repo_root
from utilities.iterables import OneEmptyError, always_iterable, one
from utilities.pathlib import ensure_suffix, resolve_path
from utilities.reprlib import (
    RICH_EXPAND_ALL,
    RICH_INDENT_SIZE,
    RICH_MAX_DEPTH,
    RICH_MAX_LENGTH,
    RICH_MAX_STRING,
    RICH_MAX_WIDTH,
)
from utilities.sentinel import Sentinel, sentinel
from utilities.traceback import RichTracebackFormatter

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Iterator
    from logging import _FilterType
    from zoneinfo import ZoneInfo

    from utilities.types import (
        LoggerOrName,
        LogLevel,
        MaybeIterable,
        PathLike,
        PathLikeOrCallable,
    )
    from utilities.version import MaybeCallableVersionLike

try:
    from whenever import ZonedDateTime
except ModuleNotFoundError:  # pragma: no cover
    ZonedDateTime = None


##


type _When = Literal[
    "S", "M", "H", "D", "midnight", "W0", "W1", "W2", "W3", "W4", "W5", "W6"
]


class SizeAndTimeRotatingFileHandler(BaseRotatingHandler):
    """Handler which rotates on size & time."""

    stream: Any

    @override
    def __init__(
        self,
        filename: PathLike,
        mode: Literal["a", "w", "x"] = "a",
        encoding: str | None = None,
        delay: bool = False,
        errors: Literal["strict", "ignore", "replace"] | None = None,
        maxBytes: int = 0,
        when: _When = "midnight",
        interval: int = 1,
        backupCount: int = 0,
        utc: bool = False,
        atTime: dt.time | None = None,
    ) -> None:
        filename = str(Path(filename))
        super().__init__(filename, mode, encoding=encoding, delay=delay, errors=errors)
        self._max_bytes = maxBytes if maxBytes >= 1 else None
        self._backup_count = backupCount if backupCount >= 1 else None
        self._filename = Path(self.baseFilename)
        self._directory = self._filename.parent
        self._stem = self._filename.stem
        self._suffix = self._filename.suffix
        self._patterns = _compute_rollover_patterns(self._stem, self._suffix)
        self._time_handler = TimedRotatingFileHandler(
            filename,
            when=when,
            interval=interval,
            backupCount=backupCount,
            encoding=encoding,
            delay=delay,
            utc=utc,
            atTime=atTime,
            errors=errors,
        )

    @override
    def emit(self, record: LogRecord) -> None:
        try:  # skipif-ci-and-windows
            if (self._backup_count is not None) and self._should_rollover(record):
                self._do_rollover(backup_count=self._backup_count)
            FileHandler.emit(self, record)
        except Exception:  # noqa: BLE001  # pragma: no cover
            self.handleError(record)

    def _do_rollover(self, *, backup_count: int = 1) -> None:
        if self.stream:  # pragma: no cover
            self.stream.close()
            self.stream = None

        actions = _compute_rollover_actions(  # skipif-ci-and-windows
            self._directory,
            self._stem,
            self._suffix,
            patterns=self._patterns,
            backup_count=backup_count,
        )
        actions.do()  # skipif-ci-and-windows

        if not self.delay:  # pragma: no cover
            self.stream = self._open()
        self._time_handler.rolloverAt = (  # skipif-ci-and-windows
            self._time_handler.computeRollover(int(time()))
        )

    def _should_rollover(self, record: LogRecord, /) -> bool:
        if self._max_bytes is not None:  # skipif-ci-and-windows
            try:
                current = self._filename.stat().st_size
            except FileNotFoundError:
                pass
            else:
                delta = len(f"{self.format(record)}\n")
                new = current + delta
                if new >= self._max_bytes:
                    return True
        return bool(self._time_handler.shouldRollover(record))  # skipif-ci-and-windows


def _compute_rollover_patterns(stem: str, suffix: str, /) -> _RolloverPatterns:
    return _RolloverPatterns(
        pattern1=re.compile(rf"^{stem}\.(\d+){suffix}$"),
        pattern2=re.compile(rf"^{stem}\.(\d+)__(\d{{8}}T\d{{6}}){suffix}$"),
        pattern3=re.compile(
            rf"^{stem}\.(\d+)__(\d{{8}}T\d{{6}})__(\d{{8}}T\d{{6}}){suffix}$"
        ),
    )


@dataclass(order=True, unsafe_hash=True, kw_only=True)
class _RolloverPatterns:
    pattern1: Pattern[str]
    pattern2: Pattern[str]
    pattern3: Pattern[str]


def _compute_rollover_actions(
    directory: Path,
    stem: str,
    suffix: str,
    /,
    *,
    patterns: _RolloverPatterns | None = None,
    backup_count: int = 1,
) -> _RolloverActions:
    patterns = (  # skipif-ci-and-windows
        _compute_rollover_patterns(stem, suffix) if patterns is None else patterns
    )
    files = {  # skipif-ci-and-windows
        file
        for path in directory.iterdir()
        if (file := _RotatingLogFile.from_path(path, stem, suffix, patterns=patterns))
        is not None
    }
    deletions: set[_Deletion] = set()  # skipif-ci-and-windows
    rotations: set[_Rotation] = set()  # skipif-ci-and-windows
    for file in files:  # skipif-ci-and-windows
        match file.index, file.start, file.end:
            case int() as index, _, _ if index >= backup_count:
                deletions.add(_Deletion(file=file))
            case index, None, _:
                if index is None:
                    curr = 0
                    end = get_now_local()
                else:
                    curr = index
                    end = sentinel
                try:
                    start = one(f for f in files if f.index == curr + 1).end
                except OneEmptyError:
                    start = None
                rotations.add(
                    _Rotation(file=file, index=curr + 1, start=start, end=end)
                )
            case int() as index, dt.datetime(), dt.datetime():
                rotations.add(_Rotation(file=file, index=index + 1))
            case _:  # pragma: no cover
                raise NotImplementedError
    return _RolloverActions(  # skipif-ci-and-windows
        deletions=deletions, rotations=rotations
    )


@dataclass(order=True, unsafe_hash=True, kw_only=True)
class _RolloverActions:
    deletions: set[_Deletion] = field(default_factory=set)
    rotations: set[_Rotation] = field(default_factory=set)

    def do(self) -> None:
        for deletion in self.deletions:  # skipif-ci-and-windows
            deletion.delete()
        move_many(  # skipif-ci-and-windows
            *((r.file.path, r.destination) for r in self.rotations)
        )


@dataclass(order=True, unsafe_hash=True, kw_only=True)
class _RotatingLogFile:
    directory: Path
    stem: str
    suffix: str
    index: int | None = None
    start: dt.datetime | None = None
    end: dt.datetime | None = None

    def __post_init__(self) -> None:
        if self.start is not None:
            self.start = round_datetime(self.start, SECOND)
        if self.end is not None:
            self.end = round_datetime(self.end, SECOND)

    @classmethod
    def from_path(
        cls,
        path: Path,
        stem: str,
        suffix: str,
        /,
        *,
        patterns: _RolloverPatterns | None = None,
    ) -> Self | None:
        if (not path.stem.startswith(stem)) or path.suffix != suffix:
            return None
        if patterns is None:  # skipif-ci-and-windows
            patterns = _compute_rollover_patterns(stem, suffix)
        try:
            (index,) = patterns.pattern1.findall(path.name)
        except ValueError:
            pass
        else:
            return cls(
                directory=path.parent, stem=stem, suffix=suffix, index=int(index)
            )
        try:
            ((index, end),) = patterns.pattern2.findall(path.name)
        except ValueError:
            pass
        else:
            return cls(
                directory=path.parent,
                stem=stem,
                suffix=suffix,
                index=int(index),
                end=parse_datetime_compact(end),
            )
        try:
            ((index, start, end),) = patterns.pattern3.findall(path.name)
        except ValueError:
            return cls(directory=path.parent, stem=stem, suffix=suffix)
        else:
            return cls(
                directory=path.parent,
                stem=stem,
                suffix=suffix,
                index=int(index),
                start=parse_datetime_compact(start),
                end=parse_datetime_compact(end),
            )

    @cached_property
    def path(self) -> Path:
        """The full path."""
        match self.index, self.start, self.end:
            case None, None, None:
                tail = None
            case int() as index, None, None:
                tail = str(index)
            case int() as index, None, dt.datetime() as end:
                tail = f"{index}__{serialize_compact(end)}"
            case int() as index, dt.datetime() as start, dt.datetime() as end:
                tail = f"{index}__{serialize_compact(start)}__{serialize_compact(end)}"
            case _:  # pragma: no cover
                raise ImpossibleCaseError(
                    case=[f"{self.index=}", f"{self.start=}", f"{self.end=}"]
                )
        stem = self.stem if tail is None else f"{self.stem}.{tail}"
        return ensure_suffix(self.directory.joinpath(stem), self.suffix)

    def replace(
        self,
        *,
        index: int | None | Sentinel = sentinel,
        start: dt.datetime | None | Sentinel = sentinel,
        end: dt.datetime | None | Sentinel = sentinel,
    ) -> Self:
        return replace_non_sentinel(  # skipif-ci-and-windows
            self, index=index, start=start, end=end
        )


@dataclass(order=True, unsafe_hash=True, kw_only=True)
class _Deletion:
    file: _RotatingLogFile

    def delete(self) -> None:
        self.file.path.unlink(missing_ok=True)  # skipif-ci-and-windows


@dataclass(order=True, unsafe_hash=True, kw_only=True)
class _Rotation:
    file: _RotatingLogFile
    index: int = 0
    start: dt.datetime | None | Sentinel = sentinel
    end: dt.datetime | Sentinel = sentinel

    def __post_init__(self) -> None:
        if isinstance(self.start, dt.datetime):  # skipif-ci-and-windows
            self.start = round_datetime(self.start, SECOND)
        if isinstance(self.end, dt.datetime):  # skipif-ci-and-windows
            self.end = round_datetime(self.end, SECOND)

    @cached_property
    def destination(self) -> Path:
        return self.file.replace(  # skipif-ci-and-windows
            index=self.index, start=self.start, end=self.end
        ).path


##


class StandaloneFileHandler(Handler):
    """Handler for emitting tracebacks to individual files."""

    @override
    def __init__(
        self, *, level: int = NOTSET, path: PathLikeOrCallable | None = None
    ) -> None:
        super().__init__(level=level)
        self._path = path

    @override
    def emit(self, record: LogRecord) -> None:
        try:
            path = (
                resolve_path(path=self._path)
                .joinpath(serialize_compact(get_now_local()))
                .with_suffix(".txt")
            )
            formatted = self.format(record)
            with writer(path, overwrite=True) as temp, temp.open(mode="w") as fh:
                _ = fh.write(formatted)
        except Exception:  # noqa: BLE001  # pragma: no cover
            self.handleError(record)


##


def add_filters(handler: Handler, /, *filters: _FilterType) -> None:
    """Add a set of filters to a handler."""
    for filter_i in filters:
        handler.addFilter(filter_i)


##


def basic_config(
    *,
    format: str = "{asctime} | {name} | {levelname:8} | {message}",  # noqa: A002
    level: LogLevel = "INFO",
) -> None:
    """Do the basic config."""
    basicConfig(
        format=format,
        datefmt=maybe_sub_pct_y("%Y-%m-%d %H:%M:%S"),
        style="{",
        level=level,
    )


##


def filter_for_key(
    key: str, /, *, default: bool = False
) -> Callable[[LogRecord], bool]:
    """Make a filter for a given attribute."""
    if (key in _FILTER_FOR_KEY_BLACKLIST) or key.startswith("__"):
        raise FilterForKeyError(key=key)

    def filter_(record: LogRecord, /) -> bool:
        try:
            value = getattr(record, key)
        except AttributeError:
            return default
        return bool(value)

    return filter_


# fmt: off
_FILTER_FOR_KEY_BLACKLIST = {
    "args", "created", "exc_info", "exc_text", "filename", "funcName", "getMessage", "levelname", "levelno", "lineno", "module", "msecs", "msg", "name", "pathname", "process", "processName", "relativeCreated", "stack_info", "taskName", "thread", "threadName"
}
# fmt: on


@dataclass(kw_only=True, slots=True)
class FilterForKeyError(Exception):
    key: str

    @override
    def __str__(self) -> str:
        return f"Invalid key: {self.key!r}"


##


def get_default_logging_path() -> Path:
    """Get the logging default path."""
    return get_repo_root().joinpath(".logs")


##


def get_logger(*, logger: LoggerOrName | None = None) -> Logger:
    """Get a logger."""
    match logger:
        case Logger():
            return logger
        case str() | None:
            return getLogger(logger)
        case _ as never:
            assert_never(never)


##


def get_logging_level_number(level: LogLevel, /) -> int:
    """Get the logging level number."""
    mapping = getLevelNamesMapping()
    try:
        return mapping[level]
    except KeyError:
        raise GetLoggingLevelNumberError(level=level) from None


@dataclass(kw_only=True, slots=True)
class GetLoggingLevelNumberError(Exception):
    level: LogLevel

    @override
    def __str__(self) -> str:
        return f"Invalid logging level: {self.level!r}"


##


def setup_logging(
    *,
    logger: LoggerOrName | None = None,
    console_level: LogLevel | None = "INFO",
    console_filters: Iterable[_FilterType] | None = None,
    console_fmt: str = "❯ {_zoned_datetime_str} | {name}:{funcName}:{lineno} | {message}",  # noqa: RUF001
    files_dir: PathLikeOrCallable | None = get_default_logging_path,
    files_when: _When = "D",
    files_interval: int = 1,
    files_backup_count: int = 10,
    files_max_bytes: int = 10 * 1024**2,
    files_filters: Iterable[_FilterType] | None = None,
    files_fmt: str = "{_zoned_datetime_str} | {name}:{funcName}:{lineno} | {levelname:8} | {message}",
    filters: MaybeIterable[_FilterType] | None = None,
    formatter_version: MaybeCallableVersionLike | None = None,
    formatter_max_width: int = RICH_MAX_WIDTH,
    formatter_indent_size: int = RICH_INDENT_SIZE,
    formatter_max_length: int | None = RICH_MAX_LENGTH,
    formatter_max_string: int | None = RICH_MAX_STRING,
    formatter_max_depth: int | None = RICH_MAX_DEPTH,
    formatter_expand_all: bool = RICH_EXPAND_ALL,
    extra: Callable[[LoggerOrName | None], None] | None = None,
) -> None:
    """Set up logger."""
    # log record factory
    from utilities.tzlocal import get_local_time_zone  # skipif-ci-and-windows

    class LogRecordNanoLocal(  # skipif-ci-and-windows
        _AdvancedLogRecord, time_zone=get_local_time_zone()
    ): ...

    setLogRecordFactory(LogRecordNanoLocal)  # skipif-ci-and-windows

    console_fmt, files_fmt = [  # skipif-ci-and-windows
        f.replace("{_zoned_datetime_str}", LogRecordNanoLocal.get_zoned_datetime_fmt())
        for f in [console_fmt, files_fmt]
    ]

    # logger
    logger_use = get_logger(logger=logger)  # skipif-ci-and-windows
    logger_use.setLevel(DEBUG)  # skipif-ci-and-windows

    # filters
    console_filters = (  # skipif-ci-and-windows
        [] if console_filters is None else list(console_filters)
    )
    files_filters = (  # skipif-ci-and-windows
        [] if files_filters is None else list(files_filters)
    )
    filters = (  # skipif-ci-and-windows
        [] if filters is None else list(always_iterable(filters))
    )

    # formatters
    try:  # skipif-ci-and-windows
        from coloredlogs import DEFAULT_FIELD_STYLES, ColoredFormatter
    except ModuleNotFoundError:  # pragma: no cover
        console_formatter = Formatter(fmt=console_fmt, style="{")
        files_formatter = Formatter(fmt=files_fmt, style="{")
    else:  # skipif-ci-and-windows
        field_styles = DEFAULT_FIELD_STYLES | {
            "_zoned_datetime_str": DEFAULT_FIELD_STYLES["asctime"]
        }
        console_formatter = ColoredFormatter(
            fmt=console_fmt, style="{", field_styles=field_styles
        )
        files_formatter = ColoredFormatter(
            fmt=files_fmt, style="{", field_styles=field_styles
        )
    plain_formatter = Formatter(fmt=files_fmt, style="{")  # skipif-ci-and-windows

    # console
    if console_level is not None:  # skipif-ci-and-windows
        console_low_handler = StreamHandler(stream=stdout)
        add_filters(console_low_handler, lambda x: x.levelno < ERROR)
        add_filters(console_low_handler, *console_filters)
        add_filters(console_low_handler, *filters)
        console_low_handler.setFormatter(console_formatter)
        console_low_handler.setLevel(console_level)
        logger_use.addHandler(console_low_handler)

        console_high_handler = StreamHandler(stream=stdout)
        add_filters(console_high_handler, *console_filters)
        add_filters(console_high_handler, *filters)
        _ = RichTracebackFormatter.create_and_set(
            console_high_handler,
            version=formatter_version,
            max_width=formatter_max_width,
            indent_size=formatter_indent_size,
            max_length=formatter_max_length,
            max_string=formatter_max_string,
            max_depth=formatter_max_depth,
            expand_all=formatter_expand_all,
            detail=True,
            post=_ansi_wrap_red,
        )
        console_high_handler.setLevel(
            max(get_logging_level_number(console_level), ERROR)
        )
        logger_use.addHandler(console_high_handler)

    # debug & info
    directory = resolve_path(path=files_dir)  # skipif-ci-and-windows
    levels: list[LogLevel] = ["DEBUG", "INFO"]  # skipif-ci-and-windows
    for level, (subpath, files_or_plain_formatter) in product(  # skipif-ci-and-windows
        levels, [(Path(), files_formatter), (Path("plain"), plain_formatter)]
    ):
        path = ensure_suffix(directory.joinpath(subpath, level.lower()), ".txt")
        path.parent.mkdir(parents=True, exist_ok=True)
        file_handler = SizeAndTimeRotatingFileHandler(
            filename=path,
            when=files_when,
            interval=files_interval,
            backupCount=files_backup_count,
            maxBytes=files_max_bytes,
        )
        add_filters(file_handler, *files_filters)
        add_filters(file_handler, *filters)
        file_handler.setFormatter(files_or_plain_formatter)
        file_handler.setLevel(level)
        logger_use.addHandler(file_handler)

    # errors
    standalone_file_handler = StandaloneFileHandler(  # skipif-ci-and-windows
        level=ERROR, path=directory.joinpath("errors")
    )
    add_filters(standalone_file_handler, lambda x: x.exc_info is not None)
    standalone_file_handler.setFormatter(
        RichTracebackFormatter(
            version=formatter_version,
            max_width=formatter_max_width,
            indent_size=formatter_indent_size,
            max_length=formatter_max_length,
            max_string=formatter_max_string,
            max_depth=formatter_max_depth,
            expand_all=formatter_expand_all,
            detail=True,
        )
    )
    logger_use.addHandler(standalone_file_handler)  # skipif-ci-and-windows

    # extra
    if extra is not None:  # skipif-ci-and-windows
        extra(logger_use)


##


@contextmanager
def temp_handler(
    handler: Handler, /, *, logger: LoggerOrName | None = None
) -> Iterator[None]:
    """Context manager with temporary handler set."""
    logger_use = get_logger(logger=logger)
    logger_use.addHandler(handler)
    try:
        yield
    finally:
        _ = logger_use.removeHandler(handler)


##


@contextmanager
def temp_logger(
    logger: LoggerOrName,
    /,
    *,
    disabled: bool | None = None,
    level: LogLevel | None = None,
    propagate: bool | None = None,
) -> Iterator[Logger]:
    """Context manager with temporary logger settings."""
    logger_use = get_logger(logger=logger)
    init_disabled = logger_use.disabled
    init_level = logger_use.level
    init_propagate = logger_use.propagate
    if disabled is not None:
        logger_use.disabled = disabled
    if level is not None:
        logger_use.setLevel(level)
    if propagate is not None:
        logger_use.propagate = propagate
    try:
        yield logger_use
    finally:
        if disabled is not None:
            logger_use.disabled = init_disabled
        if level is not None:
            logger_use.setLevel(init_level)
        if propagate is not None:
            logger_use.propagate = init_propagate


##


class _AdvancedLogRecord(LogRecord):
    """Advanced log record."""

    time_zone: ClassVar[str] = NotImplemented

    @override
    def __init__(
        self,
        name: str,
        level: int,
        pathname: str,
        lineno: int,
        msg: object,
        args: Any,
        exc_info: Any,
        func: str | None = None,
        sinfo: str | None = None,
    ) -> None:
        self._zoned_datetime = self.get_now()  # skipif-ci-and-windows
        self._zoned_datetime_str = (  # skipif-ci-and-windows
            self._zoned_datetime.format_common_iso()
        )
        super().__init__(  # skipif-ci-and-windows
            name, level, pathname, lineno, msg, args, exc_info, func, sinfo
        )

    @override
    def __init_subclass__(cls, *, time_zone: ZoneInfo, **kwargs: Any) -> None:
        cls.time_zone = time_zone.key  # skipif-ci-and-windows
        super().__init_subclass__(**kwargs)  # skipif-ci-and-windows

    @override
    def getMessage(self) -> str:
        """Return the message for this LogRecord."""
        msg = str(self.msg)  # pragma: no cover
        if self.args:  # pragma: no cover
            try:
                return msg % self.args  # compability for 3rd party code
            except ValueError as error:
                if len(error.args) == 0:
                    raise
                first = error.args[0]
                if search("unsupported format character", first):
                    return msg.format(*self.args)
                raise
            except TypeError as error:
                if len(error.args) == 0:
                    raise
                first = error.args[0]
                if search("not all arguments converted", first):
                    return msg.format(*self.args)
                raise
        return msg  # pragma: no cover

    @classmethod
    def get_now(cls) -> Any:
        """Get the current zoned datetime."""
        return cast("Any", ZonedDateTime).now(cls.time_zone)  # skipif-ci-and-windows

    @classmethod
    def get_zoned_datetime_fmt(cls) -> str:
        """Get the zoned datetime format string."""
        length = len(cls.get_now().format_common_iso())  # skipif-ci-and-windows
        return f"{{_zoned_datetime_str:{length}}}"  # skipif-ci-and-windows


##


def _ansi_wrap_red(text: str, /) -> str:
    try:
        from humanfriendly.terminal import ansi_wrap
    except ModuleNotFoundError:  # pragma: no cover
        return text
    return ansi_wrap(text, color="red")


__all__ = [
    "FilterForKeyError",
    "GetLoggingLevelNumberError",
    "SizeAndTimeRotatingFileHandler",
    "StandaloneFileHandler",
    "add_filters",
    "basic_config",
    "filter_for_key",
    "get_default_logging_path",
    "get_logger",
    "get_logging_level_number",
    "setup_logging",
    "temp_handler",
    "temp_logger",
]
