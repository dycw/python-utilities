from __future__ import annotations

import re
from dataclasses import dataclass, field
from functools import cached_property
from logging import (
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
from re import Pattern
from typing import (
    TYPE_CHECKING,
    Any,
    Literal,
    NotRequired,
    Self,
    TypedDict,
    assert_never,
    cast,
    override,
)

from whenever import PlainDateTime, ZonedDateTime

from utilities.atomicwrites import move_many
from utilities.dataclasses import replace_non_sentinel
from utilities.errors import ImpossibleCaseError
from utilities.iterables import OneEmptyError, always_iterable, one
from utilities.pathlib import ensure_suffix, get_path
from utilities.re import (
    ExtractGroupError,
    ExtractGroupsError,
    extract_group,
    extract_groups,
)
from utilities.sentinel import Sentinel, sentinel
from utilities.tzlocal import LOCAL_TIME_ZONE_NAME
from utilities.whenever import (
    WheneverLogRecord,
    format_compact,
    get_now_local,
    to_local_plain,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Mapping
    from datetime import time
    from logging import _FilterType

    from utilities.types import (
        LoggerOrName,
        LogLevel,
        MaybeCallablePathLike,
        MaybeIterable,
        PathLike,
    )


_DEFAULT_FORMAT = (
    "{zoned_datetime} | {name}:{funcName}:{lineno} | {levelname:8} | {message}"
)
_DEFAULT_DATEFMT = "%Y-%m-%d %H:%M:%S"
_DEFAULT_BACKUP_COUNT: int = 100
_DEFAULT_MAX_BYTES: int = 10 * 1024**2
_DEFAULT_WHEN: _When = "D"


##


def add_filters(handler: Handler, /, *filters: _FilterType) -> None:
    """Add a set of filters to a handler."""
    for filter_i in filters:
        handler.addFilter(filter_i)


##


def basic_config(
    *,
    obj: LoggerOrName | Handler | None = None,
    format_: str = _DEFAULT_FORMAT,
    datefmt: str = _DEFAULT_DATEFMT,
    level: LogLevel = "INFO",
    filters: MaybeIterable[_FilterType] | None = None,
    plain: bool = False,
    color_field_styles: Mapping[str, _FieldStyleKeys] | None = None,
) -> None:
    """Do the basic config."""
    match obj:
        case None:
            basicConfig(format=format_, datefmt=datefmt, style="{", level=level)
        case Logger() as logger:
            logger.setLevel(level)
            logger.addHandler(handler := StreamHandler())
            basic_config(
                obj=handler,
                format_=format_,
                datefmt=datefmt,
                level=level,
                filters=filters,
                plain=plain,
                color_field_styles=color_field_styles,
            )
        case str() as name:
            basic_config(
                obj=get_logger(logger=name),
                format_=format_,
                datefmt=datefmt,
                level=level,
                filters=filters,
                plain=plain,
                color_field_styles=color_field_styles,
            )
        case Handler() as handler:
            handler.setLevel(level)
            if filters is not None:
                add_filters(handler, *always_iterable(filters))
            formatter = get_formatter(
                format_=format_,
                datefmt=datefmt,
                plain=plain,
                color_field_styles=color_field_styles,
            )
            handler.setFormatter(formatter)
        case _ as never:
            assert_never(never)


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


type _FieldStyleKeys = Literal[
    "asctime", "hostname", "levelname", "name", "programname", "username"
]


class _FieldStyleDict(TypedDict):
    color: str
    bold: NotRequired[bool]


def get_formatter(
    *,
    format_: str = _DEFAULT_FORMAT,
    datefmt: str = _DEFAULT_DATEFMT,
    plain: bool = False,
    color_field_styles: Mapping[str, _FieldStyleKeys] | None = None,
) -> Formatter:
    """Get the formatter; colored if available."""
    setLogRecordFactory(WheneverLogRecord)
    if plain:
        return _get_plain_formatter(format_=format_, datefmt=datefmt)
    try:
        from coloredlogs import DEFAULT_FIELD_STYLES, ColoredFormatter
    except ModuleNotFoundError:  # pragma: no cover
        return _get_plain_formatter(format_=format_, datefmt=datefmt)
    default = cast("dict[_FieldStyleKeys, _FieldStyleDict]", DEFAULT_FIELD_STYLES)
    field_styles = {cast("str", k): v for k, v in default.items()}
    field_styles["zoned_datetime"] = default["asctime"]
    if color_field_styles is not None:
        field_styles.update({k: default[v] for k, v in color_field_styles.items()})
    return ColoredFormatter(
        fmt=format_, datefmt=datefmt, style="{", field_styles=field_styles
    )


def _get_plain_formatter(
    *, format_: str = _DEFAULT_FORMAT, datefmt: str = _DEFAULT_DATEFMT
) -> Formatter:
    """Get the plain formatter."""
    return Formatter(fmt=format_, datefmt=datefmt, style="{")


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
    format_: str = _DEFAULT_FORMAT,
    datefmt: str = _DEFAULT_DATEFMT,
    console_level: LogLevel = "INFO",
    console_prefix: str = "❯",  # noqa: RUF001
    console_filters: MaybeIterable[_FilterType] | None = None,
    files_dir: MaybeCallablePathLike | None = None,
    files_max_bytes: int = _DEFAULT_MAX_BYTES,
    files_when: _When = _DEFAULT_WHEN,
    files_interval: int = 1,
    files_backup_count: int = _DEFAULT_BACKUP_COUNT,
    files_filters: Iterable[_FilterType] | None = None,
) -> None:
    """Set up logger."""
    basic_config(
        obj=logger,
        format_=f"{console_prefix} {format_}",
        datefmt=datefmt,
        level=console_level,
        filters=console_filters,
    )
    logger_use = get_logger(logger=logger)
    name = logger_use.name
    dir_ = get_path(path=files_dir)
    levels: list[LogLevel] = ["DEBUG", "INFO", "ERROR"]
    for level in levels:
        lower = level.lower()
        for stem in [lower, f"{name}-{lower}"]:
            handler = SizeAndTimeRotatingFileHandler(
                dir_.joinpath(stem).with_suffix(".txt"),
                maxBytes=files_max_bytes,
                when=files_when,
                interval=files_interval,
                backupCount=files_backup_count,
            )
            logger_use.addHandler(handler)
            basic_config(
                obj=handler,
                format_=format_,
                datefmt=datefmt,
                level=level,
                filters=files_filters,
                plain=True,
            )


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
        maxBytes: int = _DEFAULT_MAX_BYTES,
        when: _When = _DEFAULT_WHEN,
        interval: int = 1,
        backupCount: int = _DEFAULT_BACKUP_COUNT,
        utc: bool = False,
        atTime: time | None = None,
    ) -> None:
        path = Path(filename)
        path.parent.mkdir(parents=True, exist_ok=True)
        super().__init__(path, mode, encoding=encoding, delay=delay, errors=errors)
        self._max_bytes = maxBytes if maxBytes >= 1 else None
        self._backup_count = backupCount if backupCount >= 1 else None
        self._filename = Path(self.baseFilename)
        self._directory = self._filename.parent
        self._stem = self._filename.stem
        self._suffix = self._filename.suffix
        self._patterns = _compute_rollover_patterns(self._stem, self._suffix)
        self._time_handler = TimedRotatingFileHandler(
            path,
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
        try:
            if (self._backup_count is not None) and self._should_rollover(record):
                self._do_rollover(  # skipif-ci-and-windows
                    backup_count=self._backup_count
                )
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
            self._time_handler.computeRollover(get_now_local().timestamp())
        )

    def _should_rollover(self, record: LogRecord, /) -> bool:
        if self._max_bytes is not None:  # skipif-ci-and-windows
            try:
                size = self._filename.stat().st_size
            except FileNotFoundError:
                pass
            else:
                if size >= self._max_bytes:
                    return True
        return bool(self._time_handler.shouldRollover(record))  # skipif-ci-and-windows


def _compute_rollover_patterns(stem: str, suffix: str, /) -> _RolloverPatterns:
    return _RolloverPatterns(
        pattern1=re.compile(rf"^{stem}\.(\d+){suffix}$"),
        pattern2=re.compile(rf"^{stem}\.(\d+)__([\dT]+?){suffix}$"),
        pattern3=re.compile(rf"^{stem}\.(\d+)__([\dT]+?)__([\dT]+?){suffix}$"),
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
    patterns = (
        _compute_rollover_patterns(stem, suffix) if patterns is None else patterns
    )
    files = {
        file
        for path in directory.iterdir()
        if (file := _RotatingLogFile.from_path(path, stem, suffix, patterns=patterns))
        is not None
    }
    deletions: set[_Deletion] = set()
    rotations: set[_Rotation] = set()
    for file in files:
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
            case int() as index, ZonedDateTime(), ZonedDateTime():
                rotations.add(_Rotation(file=file, index=index + 1))
            case _:  # pragma: no cover
                raise NotImplementedError
    return _RolloverActions(deletions=deletions, rotations=rotations)


@dataclass(order=True, unsafe_hash=True, kw_only=True)
class _RolloverActions:
    deletions: set[_Deletion] = field(default_factory=set)
    rotations: set[_Rotation] = field(default_factory=set)

    def do(self) -> None:
        for deletion in self.deletions:
            deletion.delete()
        move_many(*((r.file.path, r.destination) for r in self.rotations))


@dataclass(order=True, unsafe_hash=True, kw_only=True)
class _RotatingLogFile:
    directory: Path
    stem: str
    suffix: str
    index: int | None = None
    start: ZonedDateTime | None = None
    end: ZonedDateTime | None = None

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
        if patterns is None:
            patterns = _compute_rollover_patterns(stem, suffix)
        try:
            index, start, end = extract_groups(patterns.pattern3, path.name)
        except ExtractGroupsError:
            pass
        else:
            return cls(
                directory=path.parent,
                stem=stem,
                suffix=suffix,
                index=int(index),
                start=PlainDateTime.parse_common_iso(start).assume_tz(
                    LOCAL_TIME_ZONE_NAME
                ),
                end=PlainDateTime.parse_common_iso(end).assume_tz(LOCAL_TIME_ZONE_NAME),
            )
        try:
            index, end = extract_groups(patterns.pattern2, path.name)
        except ExtractGroupsError:
            pass
        else:
            return cls(
                directory=path.parent,
                stem=stem,
                suffix=suffix,
                index=int(index),
                end=PlainDateTime.parse_common_iso(end).assume_tz(LOCAL_TIME_ZONE_NAME),
            )
        try:
            index = extract_group(patterns.pattern1, path.name)
        except ExtractGroupError:
            pass
        else:
            return cls(
                directory=path.parent, stem=stem, suffix=suffix, index=int(index)
            )
        return cls(directory=path.parent, stem=stem, suffix=suffix)

    @cached_property
    def path(self) -> Path:
        """The full path."""
        match self.index, self.start, self.end:
            case None, None, None:
                tail = None
            case int() as index, None, None:
                tail = str(index)
            case int() as index, None, ZonedDateTime() as end:
                tail = f"{index}__{format_compact(to_local_plain(end))}"
            case int() as index, ZonedDateTime() as start, ZonedDateTime() as end:
                tail = f"{index}__{format_compact(to_local_plain(start))}__{format_compact(to_local_plain(end))}"
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
        start: ZonedDateTime | None | Sentinel = sentinel,
        end: ZonedDateTime | None | Sentinel = sentinel,
    ) -> Self:
        return replace_non_sentinel(self, index=index, start=start, end=end)


@dataclass(order=True, unsafe_hash=True, kw_only=True)
class _Deletion:
    file: _RotatingLogFile

    def delete(self) -> None:
        self.file.path.unlink(missing_ok=True)


@dataclass(order=True, unsafe_hash=True, kw_only=True)
class _Rotation:
    file: _RotatingLogFile
    index: int = 0
    start: ZonedDateTime | None | Sentinel = sentinel
    end: ZonedDateTime | Sentinel = sentinel

    @cached_property
    def destination(self) -> Path:
        return self.file.replace(index=self.index, start=self.start, end=self.end).path


__all__ = [
    "FilterForKeyError",
    "GetLoggingLevelNumberError",
    "SizeAndTimeRotatingFileHandler",
    "add_filters",
    "basic_config",
    "filter_for_key",
    "get_logger",
    "get_logging_level_number",
    "setup_logging",
]
