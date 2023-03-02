import datetime as dt
import logging
from collections.abc import Iterator, Mapping
from contextlib import suppress
from logging import Handler, LogRecord, basicConfig, getLogger
from os import environ, getenv
from pathlib import Path
from re import search
from sys import _getframe, stdout
from typing import Any, Optional, Union, cast

from beartype import beartype
from loguru import logger

from utilities.beartype import IterableStrs
from utilities.logging import LogLevel
from utilities.pathlib import PathLike
from utilities.re import NoMatchesError, extract_group

_LEVELS_ENV_VAR_PREFIX = "LOGGING"
_FILES_ENV_VAR = "LOGGING"
_ROTATION = int(1e6)
_RETENTION = dt.timedelta(weeks=1)


@beartype
def setup_loguru(
    *,
    levels: Optional[Mapping[str, LogLevel]] = None,
    levels_env_var_prefix: Optional[str] = _LEVELS_ENV_VAR_PREFIX,
    enable: Optional[IterableStrs] = None,
    console: LogLevel = LogLevel.INFO,
    files: Optional[PathLike] = None,
    files_root: Path = Path.cwd(),
    files_env_var: Optional[str] = _FILES_ENV_VAR,
    rotation: Optional[Union[str, int, dt.time, dt.timedelta]] = _ROTATION,
    retention: Optional[Union[str, int, dt.time, dt.timedelta]] = _RETENTION,
) -> None:
    """Set up `loguru` logging."""
    logger.remove()
    basicConfig(handlers=[_InterceptHandler()], level=0, force=True)
    all_levels = _augment_levels(levels=levels, env_var_prefix=levels_env_var_prefix)
    for name, level in all_levels.items():
        _setup_standard_logger(name, level)
    if enable is not None:
        for name in enable:
            logger.enable(name)
    _add_sink(stdout, console, all_levels, live=True)
    files_path = _get_files_path(files=files, env_var=files_env_var)
    if files_path is not None:
        full_files_path = files_root.joinpath(files_path)
        _add_file_sink(full_files_path, "log", LogLevel.DEBUG, all_levels, live=False)
        for level in set(LogLevel) - {LogLevel.CRITICAL}:
            _add_live_file_sink(
                full_files_path,
                level,
                all_levels,
                rotation=rotation,
                retention=retention,
            )


class _InterceptHandler(Handler):
    """Handler for intercepting standard logging messages.

    https://github.com/Delgan/loguru#entirely-compatible-with-standard-logging
    """

    def emit(self, record: LogRecord, /) -> None:
        # Get corresponding Loguru level if it exists.
        try:
            level = logger.level(record.levelname).name
        except ValueError:  # pragma: no cover
            level = record.levelno

        # Find caller from where originated the logged message.
        frame, depth = _getframe(6), 6
        while frame and frame.f_code.co_filename == logging.__file__:
            frame = frame.f_back  # pragma: no cover
            depth += 1  # pragma: no cover

        logger.opt(depth=depth, exception=record.exc_info).log(
            level, record.getMessage()
        )


@beartype
def _augment_levels(
    *,
    levels: Optional[Mapping[str, LogLevel]] = None,
    env_var_prefix: Optional[str] = _LEVELS_ENV_VAR_PREFIX,
) -> dict[str, LogLevel]:
    """Augment the mapping of levels with the env vars."""
    out: dict[str, LogLevel] = {}
    if levels is not None:
        out |= levels
    if env_var_prefix is not None:
        for key, value in environ.items():
            with suppress(NoMatchesError):
                suffix = extract_group(rf"^{env_var_prefix}_(\w+)", key)
                module = suffix.replace("__", ".").lower()
                out[module] = LogLevel[value.upper()]
    return out


@beartype
def _setup_standard_logger(name: str, level: LogLevel, /) -> None:
    """Set up the standard loggers."""
    if search("luigi", name):
        try:
            from luigi.interface import InterfaceLogging
        except ModuleNotFoundError:  # pragma: no cover
            pass
        else:
            _ = InterfaceLogging.setup()
    std_logger = getLogger(name)
    std_logger.handlers.clear()
    std_logger.setLevel(level.name)


@beartype
def _get_files_path(
    *, files: Optional[PathLike] = None, env_var: Optional[str] = _FILES_ENV_VAR
) -> Optional[PathLike]:
    """Get the path of the files, possibly from the env var."""
    if files is not None:
        return files
    if env_var is not None:
        return getenv(env_var)
    return None


@beartype
def _add_sink(
    sink: Any,
    level: LogLevel,
    levels: Mapping[str, LogLevel],
    /,
    *,
    live: bool,
    rotation: Optional[Union[str, int, dt.time, dt.timedelta]] = _ROTATION,
    retention: Optional[Union[str, int, dt.time, dt.timedelta]] = _RETENTION,
) -> None:
    """Add a sink."""
    filter_ = {name: level.name for name, level in levels.items()}
    if isinstance(sink, (Path, str)):
        rotation_kwargs = {"rotation": rotation}
        retention_kwargs = {"retention": retention}
    else:
        rotation_kwargs = retention_kwargs = {}
    _ = logger.add(
        sink,
        level=level.name,
        format=_get_format(live=live),
        filter=cast(Any, filter_),
        colorize=live,
        backtrace=True,
        enqueue=True,
        **rotation_kwargs,
        **retention_kwargs,
    )


@beartype
def _get_format(*, live: bool) -> str:
    """Get the format string."""

    @beartype
    def yield_parts() -> Iterator[str]:
        yield (
            "<green>{time:YYYY-MM-DD}</green>"
            " "
            "<bold><green>{time:HH:mm:ss}</green></bold>"
            "."
            "{time:SSS}"
            "  "
            "<bold><level>{level.name}</level></bold>"
            "  "
            "<cyan>{process.name}</cyan>-{process.id}"
            "  "
            "<green>{name}</green>-<cyan>{function}</cyan>"
        )
        yield "\n" if live else "  "
        yield "{message}"
        yield "\n" if live else ""

    return "".join(yield_parts())


@beartype
def _add_file_sink(
    path: PathLike,
    name: str,
    level: LogLevel,
    levels: Mapping[str, LogLevel],
    /,
    *,
    live: bool,
    rotation: Optional[Union[str, int, dt.time, dt.timedelta]] = _ROTATION,
    retention: Optional[Union[str, int, dt.time, dt.timedelta]] = _RETENTION,
) -> None:
    """Add a file sink."""
    _add_sink(
        Path(path, name),
        level,
        levels,
        live=live,
        rotation=rotation,
        retention=retention,
    )


@beartype
def _add_live_file_sink(
    path: PathLike,
    level: LogLevel,
    levels: Mapping[str, LogLevel],
    /,
    *,
    rotation: Optional[Union[str, int, dt.time, dt.timedelta]] = _ROTATION,
    retention: Optional[Union[str, int, dt.time, dt.timedelta]] = _RETENTION,
) -> None:
    """Add a live file sink."""
    _add_file_sink(
        path,
        level.name.lower(),
        level,
        levels,
        live=True,
        rotation=rotation,
        retention=retention,
    )
