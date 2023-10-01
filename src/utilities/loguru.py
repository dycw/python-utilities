from __future__ import annotations

import datetime as dt
import logging
from collections.abc import Iterator
from collections.abc import Mapping
from contextlib import suppress
from logging import Handler
from logging import LogRecord
from logging import basicConfig
from logging import getLogger
from os import environ
from os import getenv
from pathlib import Path
from re import search
from sys import _getframe
from sys import stdout
from typing import Any
from typing import TypedDict
from typing import cast

from loguru import logger
from typing_extensions import override

from utilities.logging import LogLevel
from utilities.pathlib import PathLike
from utilities.re import NoMatchesError
from utilities.re import extract_group
from utilities.typing import IterableStrs

_LEVELS_ENV_VAR_PREFIX = "LOGGING"
_FILES_ENV_VAR = "LOGGING"
_ROTATION = int(1e6)
_RETENTION = dt.timedelta(weeks=1)


def setup_loguru(
    *,
    levels: Mapping[str, LogLevel] | None = None,
    levels_env_var_prefix: str | None = _LEVELS_ENV_VAR_PREFIX,
    enable: IterableStrs | None = None,
    console: LogLevel = LogLevel.INFO,
    files: PathLike | None = None,
    files_root: Path = Path.cwd(),
    files_env_var: str | None = _FILES_ENV_VAR,
    rotation: str | int | dt.time | dt.timedelta | None = _ROTATION,
    retention: str | int | dt.timedelta | None = _RETENTION,
) -> None:
    """Set up `loguru` logging."""
    logger.remove()
    basicConfig(handlers=[_InterceptHandler()], level=0, force=True)
    all_levels = _augment_levels(
        levels=levels, env_var_prefix=levels_env_var_prefix
    )
    for name, level in all_levels.items():
        _setup_standard_logger(name, level)
    if enable is not None:
        for name in enable:
            logger.enable(name)
    _add_sink(stdout, console, all_levels, live=True)
    files_path = _get_files_path(files=files, env_var=files_env_var)
    if files_path is not None:
        full_files_path = files_root.joinpath(files_path)
        _add_file_sink(
            full_files_path, "log", LogLevel.DEBUG, all_levels, live=False
        )
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

    @override
    def emit(self, record: LogRecord) -> None:
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


def _augment_levels(
    *,
    levels: Mapping[str, LogLevel] | None = None,
    env_var_prefix: str | None = _LEVELS_ENV_VAR_PREFIX,
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


def _get_files_path(
    *, files: PathLike | None = None, env_var: str | None = _FILES_ENV_VAR
) -> PathLike | None:
    """Get the path of the files, possibly from the env var."""
    if files is not None:
        return files
    if env_var is not None:
        return getenv(env_var)
    return None


def _add_sink(
    sink: Any,
    level: LogLevel,
    levels: Mapping[str, LogLevel],
    /,
    *,
    live: bool,
    rotation: str | int | dt.time | dt.timedelta | None = _ROTATION,
    retention: str | int | dt.timedelta | None = _RETENTION,
) -> None:
    """Add a sink."""
    filter_ = {name: level.name for name, level in levels.items()}

    class Kwargs(TypedDict, total=False):
        rotation: str | int | dt.time | dt.timedelta | None
        retention: str | int | dt.timedelta | None

    if isinstance(sink, Path | str):
        kwargs = cast(Kwargs, {"rotation": rotation, "retention": retention})
    else:
        kwargs = cast(Kwargs, {})
    _ = logger.add(
        sink,
        level=level.name,
        format=_get_format(live=live),
        filter=cast(Any, filter_),
        colorize=live,
        backtrace=True,
        enqueue=True,
        **kwargs,
    )


def _get_format(*, live: bool) -> str:
    """Get the format string."""

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


def _add_file_sink(
    path: PathLike,
    name: str,
    level: LogLevel,
    levels: Mapping[str, LogLevel],
    /,
    *,
    live: bool,
    rotation: str | int | dt.time | dt.timedelta | None = _ROTATION,
    retention: str | int | dt.timedelta | None = _RETENTION,
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


def _add_live_file_sink(
    path: PathLike,
    level: LogLevel,
    levels: Mapping[str, LogLevel],
    /,
    *,
    rotation: str | int | dt.time | dt.timedelta | None = _ROTATION,
    retention: str | int | dt.timedelta | None = _RETENTION,
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
