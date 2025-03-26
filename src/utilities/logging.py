from __future__ import annotations

import datetime as dt
import re
from contextlib import contextmanager, suppress
from dataclasses import InitVar, dataclass, field
from functools import cached_property
from itertools import product
from logging import (
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
from logging.handlers import (
    BaseRotatingHandler,
    RotatingFileHandler,
    TimedRotatingFileHandler,
)
from pathlib import Path
from re import Pattern, search
from shutil import move
from sys import base_exec_prefix, exception, stdout
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    Literal,
    Self,
    TextIO,
    assert_never,
    cast,
    override,
)

from utilities.atomicwrites import writer
from utilities.dataclasses import replace_non_sentinel
from utilities.datetime import get_now, maybe_sub_pct_y
from utilities.errors import ImpossibleCaseError
from utilities.git import MASTER, get_repo_root
from utilities.iterables import OneEmptyError, one
from utilities.pathlib import ensure_suffix, resolve_path
from utilities.sentinel import Sentinel, sentinel
from utilities.traceback import RichTracebackFormatter
from utilities.types import LogLevel, PathLike

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Iterator, Sequence
    from io import TextIOWrapper
    from logging import _FilterType
    from zoneinfo import ZoneInfo

    from utilities.types import LoggerOrName, PathLikeOrCallable

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

    stream: TextIO | None

    @override
    def __init__(
        self,
        filename: PathLike,
        mode: Literal["a", "w", "x"] = "a",
        maxBytes: int = 0,
        backupCount: int = 0,
        delay: bool = True,  # set to True
        errors: Literal["strict", "ignore", "replace"] | None = None,
        when: _When = "midnight",
        interval: int = 1,
        encoding: str | None = None,
        utc: bool = False,
        atTime: dt.time | None = None,
    ) -> None:
        filename = str(Path(filename))
        super().__init__(filename, mode, encoding=encoding, delay=delay)
        self._backup_count = backupCount
        self._filename = Path(self.baseFilename)
        self._directory = self._filename.parent
        self._stem = self._filename.stem
        self._suffix = self._filename.suffix
        self._patterns = _compute_rollover_patterns(self._stem, self._suffix)
        self._size_handler = RotatingFileHandler(
            filename,
            mode=mode,
            maxBytes=maxBytes,
            backupCount=backupCount,
            encoding=encoding,
            delay=True,
            errors=errors,
        )
        self._time_handler = TimedRotatingFileHandler(
            filename,
            when=when,
            interval=interval,
            backupCount=backupCount,
            encoding=encoding,
            delay=True,
            utc=utc,
            atTime=atTime,
            errors=errors,
        )

    @override
    def emit(self, record: LogRecord) -> None:
        # try:
        if self._should_rollover(record):
            self._do_rollover()
        FileHandler.emit(self, record)
        # except Exception:
        #     self.handleError(record)

    def _should_rollover(self, record: LogRecord, /) -> bool:
        return bool(self._size_handler.shouldRollover(record)) or bool(
            self._time_handler.shouldRollover(record)
        )

    def _do_rollover(self) -> None:
        if self.stream:
            self.stream.close()
            self.stream = None

        if self._backup_count >= 1:
            actions = _compute_rollover_actions(
                self._directory,
                self._stem,
                self._backup_count,
                patterns=self._patterns,
            )
            details = {
                _RotatingLogFile(
                    dir_=self._directory,
                    base=self._stem,
                    path=p.name,
                    _pattern1=self._pattern1,
                    _pattern2=self._pattern2,
                    _pattern3=self._pattern3,
                )
                for p in self._directory.iterdir()
                if p.name.startswith(self._stem)
            }
            try:
                detail = one(details)
            except OneEmptyError:
                detail = None

            zz
            breakpoint()

        breakpoint()
        # Backup rotation like RotatingFileHandler
        for i in range(self.backup_count - 1, 0, -1):
            sfn = f"{self.baseFilename}.{i}"
            dfn = f"{self.baseFilename}.{i + 1}"
            if os.path.exists(sfn):
                os.rename(sfn, dfn)

        dfn = f"{self.baseFilename}.1"
        if os.path.exists(self.baseFilename):
            os.rename(self.baseFilename, dfn)

        self.stream = self._open()

        # size....

        # if self.backupCount > 0:
        #     for i in range(self.backupCount - 1, 0, -1):
        #         sfn = self.rotation_filename("%s.%d" % (self.baseFilename, i))
        #         dfn = self.rotation_filename("%s.%d" % (self.baseFilename, i + 1))
        #         if os.path.exists(sfn):
        #             if os.path.exists(dfn):
        #                 os.remove(dfn)
        #             os.rename(sfn, dfn)
        #     dfn = self.rotation_filename(self.baseFilename + ".1")
        #     if os.path.exists(dfn):
        #         os.remove(dfn)
        #     self.rotate(self.baseFilename, dfn)
        # if not self.delay:
        #     self.stream = self._open()

        # currentTime = int(time.time())
        # t = self.rolloverAt - self.interval
        # if self.utc:
        #     timeTuple = time.gmtime(t)
        # else:
        #     timeTuple = time.localtime(t)
        #     dstNow = time.localtime(currentTime)[-1]
        #     dstThen = timeTuple[-1]
        #     if dstNow != dstThen:
        #         addend = 3600 if dstNow else -3600
        #         timeTuple = time.localtime(t + addend)
        # dfn = self.rotation_filename(
        #     self.baseFilename + "." + time.strftime(self.suffix, timeTuple)
        # )
        # if os.path.exists(dfn):
        #     # Already rolled over.
        #     return
        #
        # if self.stream:
        #     self.stream.close()
        #     self.stream = None
        # self.rotate(self.baseFilename, dfn)
        # if self.backupCount > 0:
        #     for s in self.getFilesToDelete():
        #         os.remove(s)
        # if not self.delay:
        #     self.stream = self._open()
        # self.rolloverAt = self.computeRollover(currentTime)


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
) -> Sequence[_Deletion | _Rotation]:
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
            case None, None, None:
                try:
                    index1 = one(f for f in files if f.index == 1)
                except OneEmptyError:
                    rotations.add(_Rotation(file=file, index=1))
                else:
                    raise NotImplementedError
            case int() as index, _, _ if index >= backup_count:
                deletions.add(_Deletion(file=file))
            case int(), dt.datetime(), dt.datetime():
                raise NotImplementedError
            case _:
                raise NotImplementedError
    for deletion in deletions:
        directory.joinpath(deletion.file.path).unlink(missing_ok=True)
    for rotation in sorted(rotations, key=lambda r: r.index, reverse=True):
        rotation.rotate()
        from_ = directory.joinpath(rotation.file.path).unlink(missing_ok=True)

    breakpoint()


@dataclass(order=True, unsafe_hash=True, kw_only=True)
class _RotatingLogFile:
    directory: Path
    stem: str
    suffix: str
    index: int | None = None
    start: dt.datetime | None = None
    end: dt.datetime | None = None

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
            (index,) = patterns.pattern1.findall(path.name)
        except ValueError:
            pass
        else:
            return cls(
                directory=path.parent,
                stem=stem,
                suffix=suffix,
                index=int(index),
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
                end=dt.datetime.strptime(end, "%Y%m%dT%H%M%S"),  # noqa: DTZ007
            )
        try:
            ((index, start, end),) = patterns.pattern3.findall(path.name)
        except ValueError:
            return cls(
                directory=path.parent,
                stem=stem,
                suffix=suffix,
            )
        else:
            return cls(
                directory=path.parent,
                stem=stem,
                suffix=suffix,
                index=int(index),
                start=dt.datetime.strptime(start, "%Y%m%dT%H%M%S"),  # noqa: DTZ007
                end=dt.datetime.strptime(end, "%Y%m%dT%H%M%S"),  # noqa: DTZ007
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
                tail = f"{index}__{end:%Y%m%dT%H%M%S}"
            case int() as index, dt.datetime() as start, dt.datetime() as end:
                tail = f"{index}__{start:%Y%m%dT%H%M%S}__{end:%Y%m%dT%H%M%S}"
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
        return replace_non_sentinel(self, index=index, start=start, end=end)

    def _compute_metadata(
        self, patterns: _RolloverPatterns, /
    ) -> tuple[int, dt.datetime | None, dt.datetime | None]:
        with suppress(ValueError):
            ((index,),) = patterns.pattern1.findall(self.path)
            return int(index), None, None
        with suppress(ValueError):
            ((index, start),) = patterns.pattern2.findall(self.path)
            return (
                int(index),
                dt.datetime.strptime(start, "%Y%m%dT%H%M%S"),  # noqa: DTZ007
                None,
            )
        with suppress(ValueError):
            ((index, start, end),) = patterns.pattern3.findall(self.path)
            return (
                int(index),
                dt.datetime.strptime(start, "%Y%m%dT%H%M%S"),  # noqa: DTZ007
                dt.datetime.strptime(end, "%Y%m%dT%H%M%S"),  # noqa: DTZ007
            )
        return 0, None, None


@dataclass(order=True, unsafe_hash=True, kw_only=True)
class _Deletion:
    file: _RotatingLogFile


@dataclass(order=True, unsafe_hash=True, kw_only=True)
class _Rotation:
    directory: Path
    file: _RotatingLogFile
    index: int = 0
    start: dt.datetime | None | Sentinel = sentinel
    end: dt.datetime = field(default_factory=get_now)

    @cached_property
    def new_name(self) -> str:
        return self.file.replace(
            index=self.index,
            start=self.start,
            end=self.end,
        ).path

    def rotate(self) -> None:
        source = self.directory.joinpath(self.file.path)
        dest = self.directory.joinpath(self.new_name)
        breakpoint()

        with writer(self.new_name) as tmp_path:
            move(self.file)


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
                .joinpath(get_now(time_zone="local").strftime("%Y-%m-%dT%H-%M-%S"))
                .with_suffix(".txt")
            )
            formatted = self.format(record)
            with writer(path, overwrite=True) as temp, temp.open(mode="w") as fh:
                _ = fh.write(formatted)
        except Exception:  # noqa: BLE001 # pragma: no cover
            self.handleError(record)


##


def add_filters(
    handler: Handler, /, *, filters: Iterable[_FilterType] | None = None
) -> None:
    """Add a set of filters to a handler."""
    if filters is not None:
        for filter_ in filters:
            handler.addFilter(filter_)


##


def basic_config(
    *,
    format: str = "{asctime} | {name} | {levelname:8} | {message}",  # noqa: A002
) -> None:
    """Do the basic config."""
    basicConfig(
        format=format,
        datefmt=maybe_sub_pct_y("%Y-%m-%d %H:%M:%S"),
        style="{",
        level="DEBUG",
    )


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
    git_ref: str = MASTER,
    files_dir: PathLikeOrCallable | None = get_default_logging_path,
    files_when: _When = "D",
    files_interval: int = 1,
    files_backup_count: int = 10,
    files_max_bytes: int = 10 * 1024**2,
    files_filters: Iterable[_FilterType] | None = None,
    files_fmt: str = "{_zoned_datetime_str} | {name}:{funcName}:{lineno} | {levelname:8} | {message}",
    filters: Iterable[_FilterType] | None = None,
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
    logger_use.setLevel(get_logging_level_number("DEBUG"))  # skipif-ci-and-windows

    # filters
    console_filters = (  # skipif-ci-and-windows
        None if console_filters is None else list(console_filters)
    )
    files_filters = (  # skipif-ci-and-windows
        None if files_filters is None else list(files_filters)
    )
    filters = None if filters is None else list(filters)  # skipif-ci-and-windows

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
        add_filters(console_low_handler, filters=[lambda x: x.levelno < ERROR])
        add_filters(console_low_handler, filters=console_filters)
        add_filters(console_low_handler, filters=filters)
        console_low_handler.setFormatter(console_formatter)
        console_low_handler.setLevel(get_logging_level_number(console_level))
        logger_use.addHandler(console_low_handler)

        console_high_handler = StreamHandler(stream=stdout)
        add_filters(console_high_handler, filters=console_filters)
        add_filters(console_high_handler, filters=filters)
        _ = RichTracebackFormatter.create_and_set(
            console_high_handler, git_ref=git_ref, detail=True, post=_ansi_wrap_red
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
        add_filters(file_handler, filters=files_filters)
        add_filters(file_handler, filters=filters)
        file_handler.setFormatter(files_or_plain_formatter)
        file_handler.setLevel(level)
        logger_use.addHandler(file_handler)

    # errors
    standalone_file_handler = StandaloneFileHandler(  # skipif-ci-and-windows
        level=ERROR, path=directory.joinpath("errors")
    )
    add_filters(standalone_file_handler, filters=[lambda x: x.exc_info is not None])
    standalone_file_handler.setFormatter(
        RichTracebackFormatter(git_ref=git_ref, detail=True)
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
    "GetLoggingLevelNumberError",
    "LogLevel",
    "SizeAndTimeRotatingFileHandler",
    "StandaloneFileHandler",
    "add_filters",
    "basic_config",
    "get_default_logging_path",
    "get_logger",
    "get_logging_level_number",
    "setup_logging",
    "temp_handler",
    "temp_logger",
]
