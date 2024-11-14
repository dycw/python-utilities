from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from itertools import product
from logging import (
    Formatter,
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
from typing import TYPE_CHECKING, Any, ClassVar, Literal, assert_never, cast

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


LogLevel = Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]


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


def _setup_logging_default_path() -> Path:
    return get_repo_root().joinpath(".logs")


def setup_logging(
    *,
    logger_name: str | None = None,
    console_level: LogLevel = "INFO",
    console_fmt: str = "❯ {zoned_datetime_str} | {name}:{funcName}:{lineno} | {message}",  # noqa: RUF001
    files_dir: PathLike | Callable[[], Path] | None = _setup_logging_default_path,
    files_when: str = "D",
    files_interval: int = 1,
    files_backup_count: int = 10,
    files_max_bytes: int = 10 * 1024**2,
    files_fmt: str = "{zoned_datetime_str} | {name}:{funcName}:{lineno} | {levelname:8} | {message}",
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
    console_handler = StreamHandler(stream=stdout)  # skipif-ci-and-windows
    console_handler.setFormatter(console_formatter)  # skipif-ci-and-windows
    console_handler.setLevel(
        get_logging_level_number(console_level)
    )  # skipif-ci-and-windows
    logger.addHandler(console_handler)  # skipif-ci-and-windows

    # files
    match files_dir:  # skipif-ci-and-windows
        case None:
            directory = Path.cwd()
        case Path() | str():
            directory = Path(files_dir)
        case Callable():
            directory = files_dir()
        case _ as never:  # pyright: ignore[reportUnnecessaryComparison]
            assert_never(never)
    levels: list[LogLevel] = ["DEBUG", "INFO", "ERROR"]  # skipif-ci-and-windows
    for level, (subpath, formatter) in product(  # skipif-ci-and-windows
        levels, [(Path(), files_formatter), (Path("plain"), plain_formatter)]
    ):
        path = directory.joinpath(subpath, level.lower())
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
        file_handler.setFormatter(formatter)
        file_handler.setLevel(level)
        logger.addHandler(file_handler)


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
        msg = str(self.msg)
        if self.args:
            try:
                return msg % self.args  # compability for 3rd party code
            except ValueError as error:
                if len(error.args) == 0:  # pragma: no cover
                    raise
                first = error.args[0]
                if search("unsupported format character", first):
                    return msg.format(*self.args)
                raise  # pragma: no cover
            except TypeError as error:  # pragma: no cover
                if len(error.args) == 0:
                    raise
                first = error.args[0]
                if search("not all arguments converted", first):
                    return msg.format(*self.args)
                raise
        return msg

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
    "basic_config",
    "get_logging_level_number",
    "setup_logging",
]
