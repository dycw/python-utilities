from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from itertools import product
from logging import (
    ERROR,
    NOTSET,
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
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path
from re import search
from sys import stdout
from typing import TYPE_CHECKING, Any, ClassVar, assert_never, cast

from typing_extensions import override

from utilities.atomicwrites import writer
from utilities.datetime import get_now, maybe_sub_pct_y
from utilities.git import MASTER, get_repo_root
from utilities.pathlib import ensure_suffix, resolve_path
from utilities.traceback import RichTracebackFormatter
from utilities.types import LogLevel

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Iterator
    from logging import _FilterType
    from zoneinfo import ZoneInfo

    from utilities.types import LoggerOrName, PathLikeOrCallable

try:
    from whenever import ZonedDateTime
except ModuleNotFoundError:  # pragma: no cover
    ZonedDateTime = None


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
    files_when: str = "D",
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
        try:
            from concurrent_log_handler import ConcurrentTimedRotatingFileHandler
        except ModuleNotFoundError:  # pragma: no cover
            file_handler = TimedRotatingFileHandler(
                filename=str(path),
                when=files_when,
                interval=files_interval,
                backupCount=files_backup_count,
            )
        else:
            file_handler = ConcurrentTimedRotatingFileHandler(
                filename=str(path),
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
        return cast(Any, ZonedDateTime).now(cls.time_zone)  # skipif-ci-and-windows

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
