from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum, unique
from logging import (
    LogRecord,
    StreamHandler,
    basicConfig,
    getLevelNamesMapping,
    getLogger,
    setLogRecordFactory,
)
from pathlib import Path
from sys import stdout
from typing import TYPE_CHECKING, Any, ClassVar, assert_never, cast

from typing_extensions import override

from utilities.datetime import maybe_sub_pct_y
from utilities.git import get_repo_root

if TYPE_CHECKING:
    from zoneinfo import ZoneInfo

    from utilities.types import PathLike

try:
    from whenever import ZonedDateTime
except ModuleNotFoundError:  # pragma: no cover
    ZonedDateTime = None


def basic_config(
    *,
    format: str = "{asctime} | {name} | {levelname:8} | {message}",  # noqa: A002
) -> None:
    """Do the basic config."""
    basicConfig(
        format=format,
        datefmt=maybe_sub_pct_y("%Y-%m-%d %H:%M:%S"),
        style="{",
        level=LogLevel.DEBUG.name,
    )


def get_logging_level_number(level: str, /) -> int:
    """Get the logging level number."""
    mapping = getLevelNamesMapping()
    try:
        return mapping[level]
    except KeyError:
        raise GetLoggingLevelNumberError(level=level) from None


@dataclass(kw_only=True, slots=True)
class GetLoggingLevelNumberError(Exception):
    level: str

    @override
    def __str__(self) -> str:
        return f"Invalid logging level: {self.level!r}"


@unique
class LogLevel(StrEnum):
    """An enumeration of the logging levels."""

    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


class LogRecordZonedDateTime(LogRecord):
    """Subclass of LogRecord that supports zoned datetimes."""

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
        self.zoned_datetime = self.get_now()
        self.zoned_datetime_str = self.zoned_datetime.format_common_iso()
        super().__init__(
            name, level, pathname, lineno, msg, args, exc_info, func, sinfo
        )

    @override
    def __init_subclass__(cls, *, time_zone: ZoneInfo, **kwargs: Any) -> None:
        cls.time_zone = time_zone.key
        super().__init_subclass__(**kwargs)

    @classmethod
    def get_now(cls) -> Any:
        """Get the current zoned datetime."""
        return cast(Any, ZonedDateTime).now(cls.time_zone)

    @classmethod
    def get_zoned_datetime_fmt(cls) -> str:
        """Get the zoned datetime format string."""
        length = len(cls.get_now().format_common_iso())
        return f"{{zoned_datetime_str:{length}}}"


def _setup_logging_default_path() -> Path:
    return get_repo_root().joinpath(".logs")


def setup_logging(
    *,
    logger_name: str | None = None,
    fmt: str = "{zoned_datetime_str} | {name}:{funcName} | {levelname:8} | {message}",
    console_level: LogLevel = LogLevel.INFO,
    files_dir: PathLike | Callable[[], Path] | None = _setup_logging_default_path,
    files_when: str = "D",
    files_interval: int = 1,
    files_backup_count: int = 10,
    files_max_bytes: int = 10 * 1024**2,
) -> None:
    """Set up logger."""
    # log record factory
    from tzlocal import get_localzone

    class LogRecordNanoLocal(LogRecordZonedDateTime, time_zone=get_localzone()): ...

    setLogRecordFactory(LogRecordNanoLocal)

    fmt_use = fmt.replace(
        "{zoned_datetime_str}", LogRecordNanoLocal.get_zoned_datetime_fmt()
    )

    # logger
    logger = getLogger(name=logger_name)
    logger.setLevel(get_logging_level_number(LogLevel.DEBUG))

    # formatter
    try:
        from coloredlogs import DEFAULT_FIELD_STYLES, ColoredFormatter
    except ModuleNotFoundError:  # pragma: no cover
        from logging import Formatter

        console_formatter = Formatter(fmt_use, style="{")
        file_formatter = Formatter(fmt_use, style="{")
    else:
        field_styles = DEFAULT_FIELD_STYLES | {
            "zoned_datetime": DEFAULT_FIELD_STYLES["asctime"],
            "zoned_datetime_str": DEFAULT_FIELD_STYLES["asctime"],
        }
        console_formatter = ColoredFormatter(
            fmt_use, style="{", field_styles=field_styles
        )
        file_formatter = ColoredFormatter(fmt_use, style="{", field_styles=field_styles)

    # console
    console_handler = StreamHandler(stream=stdout)
    console_handler.setFormatter(console_formatter)
    console_handler.setLevel(get_logging_level_number(console_level))
    logger.addHandler(console_handler)

    # files
    match files_dir:
        case None:
            directory = Path.cwd()
        case Path() | str():
            directory = Path(files_dir)
        case Callable():
            directory = files_dir()
        case _ as never:  # pyright: ignore[reportUnnecessaryComparison]
            assert_never(never)
    directory.mkdir(parents=True, exist_ok=True)
    for level in [LogLevel.DEBUG, LogLevel.INFO, LogLevel.ERROR]:
        filename = str(directory.joinpath(level.name.lower()))
        try:
            from concurrent_log_handler import ConcurrentTimedRotatingFileHandler
        except ModuleNotFoundError:  # pragma: no cover
            from logging.handlers import TimedRotatingFileHandler

            file_handler = TimedRotatingFileHandler(
                filename=filename,
                when=files_when,
                interval=files_interval,
                backupCount=files_backup_count,
            )
        else:
            file_handler = ConcurrentTimedRotatingFileHandler(
                filename=filename,
                when=files_when,
                interval=files_interval,
                backupCount=files_backup_count,
                maxBytes=files_max_bytes,
            )
        file_handler.setFormatter(file_formatter)
        file_handler.setLevel(level)
        logger.addHandler(file_handler)


__all__ = [
    "GetLoggingLevelNumberError",
    "LogLevel",
    "LogRecordZonedDateTime",
    "basic_config",
    "get_logging_level_number",
    "setup_logging",
]
