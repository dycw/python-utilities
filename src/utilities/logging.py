from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from itertools import product
from logging import (
    ERROR,
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
from typing import TYPE_CHECKING, Any, ClassVar, Literal, TypeAlias, assert_never, cast

from typing_extensions import override

from utilities.datetime import maybe_sub_pct_y
from utilities.git import get_repo_root
from utilities.pathlib import ensure_suffix, resolve_path
from utilities.traceback import TracebackHandler

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Iterator
    from logging import _FilterType
    from zoneinfo import ZoneInfo

    from utilities.types import PathLikeOrCallable

try:
    from whenever import ZonedDateTime
except ModuleNotFoundError:  # pragma: no cover
    ZonedDateTime = None


LogLevel: TypeAlias = Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
LoggerOrName: TypeAlias = Logger | str


def add_filters(
    handler: Handler, /, *, filters: Iterable[_FilterType] | None = None
) -> None:
    """Add a set of filters to a handler."""
    if filters is not None:
        for filter_ in filters:
            handler.addFilter(filter_)


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


def get_default_logging_path() -> Path:
    """Get the logging default path."""
    return get_repo_root().joinpath(".logs")


def get_logger(logger: LoggerOrName, /) -> Logger:
    """Get a logger."""
    match logger:
        case Logger():
            return logger
        case str():
            return getLogger(logger)
        case _ as never:  # pyright: ignore[reportUnnecessaryComparison]
            assert_never(never)


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


def setup_logging(
    *,
    logger_name: str | None = None,
    console_level: LogLevel | None = "INFO",
    console_filters: Iterable[_FilterType] | None = None,
    console_fmt: str = "❯ {zoned_datetime_str} | {name}:{funcName}:{lineno} | {message}",  # noqa: RUF001
    files_dir: PathLikeOrCallable | None = get_default_logging_path,
    files_when: str = "D",
    files_interval: int = 1,
    files_backup_count: int = 10,
    files_max_bytes: int = 10 * 1024**2,
    files_filters: Iterable[_FilterType] | None = None,
    files_fmt: str = "{zoned_datetime_str} | {name}:{funcName}:{lineno} | {levelname:8} | {message}",
    filters: Iterable[_FilterType] | None = None,
    extra: Callable[[Logger], None] | None = None,
) -> None:
    """Set up logger."""
    # log record factory
    from tzlocal import get_localzone  # skipif-ci-and-windows

    class LogRecordNanoLocal(  # skipif-ci-and-windows
        _AdvancedLogRecord, time_zone=get_localzone()
    ): ...

    setLogRecordFactory(LogRecordNanoLocal)  # skipif-ci-and-windows

    console_fmt, files_fmt = [  # skipif-ci-and-windows
        f.replace("{zoned_datetime_str}", LogRecordNanoLocal.get_zoned_datetime_fmt())
        for f in [console_fmt, files_fmt]
    ]

    # logger
    logger = getLogger(name=logger_name)  # skipif-ci-and-windows
    logger.setLevel(get_logging_level_number("DEBUG"))  # skipif-ci-and-windows

    # filters
    console_filters = (  # skipif-ci-and-windows
        None if console_filters is None else list(console_filters)
    )
    files_filters = (  # skipif-ci-and-windows
        None if files_filters is None else list(files_filters)
    )
    filters = None if filters is None else list(filters)  # skipif-ci-and-windows

    # formatter
    try:  # skipif-ci-and-windows
        from coloredlogs import DEFAULT_FIELD_STYLES, ColoredFormatter
    except ModuleNotFoundError:  # pragma: no cover
        console_formatter = Formatter(fmt=console_fmt, style="{")
        files_formatter = Formatter(fmt=files_fmt, style="{")
    else:  # skipif-ci-and-windows
        field_styles = DEFAULT_FIELD_STYLES | {
            "zoned_datetime_str": DEFAULT_FIELD_STYLES["asctime"]
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
        console_handler = StreamHandler(stream=stdout)
        add_filters(console_handler, filters=console_filters)
        add_filters(console_handler, filters=filters)
        console_handler.setFormatter(console_formatter)
        console_handler.setLevel(get_logging_level_number(console_level))
        logger.addHandler(console_handler)

    # debug & info
    directory = resolve_path(path=files_dir)
    levels: list[LogLevel] = ["DEBUG", "INFO"]  # skipif-ci-and-windows
    for level, (subpath, formatter) in product(  # skipif-ci-and-windows
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
        file_handler.setFormatter(formatter)
        file_handler.setLevel(level)
        logger.addHandler(file_handler)

    # errors
    traceback_handler = TracebackHandler(  # skipif-ci-and-windows
        level=ERROR, path=directory.joinpath("errors")
    )
    logger.addHandler(traceback_handler)  # skipif-ci-and-windows

    # extra
    if extra is not None:  # skipif-ci-and-windows
        extra(logger)


@contextmanager
def temp_handler(logger: LoggerOrName, handler: Handler, /) -> Iterator[None]:
    """Context manager with temporary handler set."""
    logger = get_logger(logger)
    logger.addHandler(handler)
    try:
        yield
    finally:
        _ = logger.removeHandler(handler)


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
    logger = get_logger(logger)
    init_disabled = logger.disabled
    init_level = logger.level
    init_propagate = logger.propagate
    if disabled is not None:
        logger.disabled = disabled
    if level is not None:
        logger.setLevel(level)
    if propagate is not None:
        logger.propagate = propagate
    try:
        yield logger
    finally:
        if disabled is not None:
            logger.disabled = init_disabled
        if level is not None:
            logger.setLevel(init_level)
        if propagate is not None:
            logger.propagate = init_propagate


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
        self.zoned_datetime = self.get_now()  # skipif-ci-and-windows
        self.zoned_datetime_str = (  # skipif-ci-and-windows
            self.zoned_datetime.format_common_iso()
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
        return f"{{zoned_datetime_str:{length}}}"  # skipif-ci-and-windows


__all__ = [
    "GetLoggingLevelNumberError",
    "LogLevel",
    "add_filters",
    "basic_config",
    "get_default_logging_path",
    "get_logger",
    "get_logging_level_number",
    "setup_logging",
    "temp_handler",
    "temp_logger",
]
