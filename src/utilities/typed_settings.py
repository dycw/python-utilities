from __future__ import annotations

from dataclasses import dataclass
from functools import partial
from ipaddress import IPv4Address, IPv6Address
from os import environ
from pathlib import Path
from re import search
from typing import TYPE_CHECKING, Any, assert_never, override

import typed_settings
from typed_settings import EnvLoader, FileLoader, find
from typed_settings.converters import TSConverter
from typed_settings.loaders import TomlFormat
from whenever import (
    Date,
    DateDelta,
    DateTimeDelta,
    MonthDay,
    PlainDateTime,
    Time,
    TimeDelta,
    YearMonth,
    ZonedDateTime,
)

from utilities.iterables import always_iterable
from utilities.pathlib import get_path
from utilities.string import substitute_environ

if TYPE_CHECKING:
    from collections.abc import Callable

    from typed_settings.loaders import Loader
    from typed_settings.processors import Processor

    from utilities.types import MaybeCallablePathLike, MaybeIterable, PathLike


##


class ExtendedTSConverter(TSConverter):
    """An extension of the TSConverter for custom types."""

    @override
    def __init__(
        self,
        *,
        resolve_paths: bool = True,
        strlist_sep: str | Callable[[str], list] | None = ":",
    ) -> None:
        super().__init__(resolve_paths=resolve_paths, strlist_sep=strlist_sep)
        cases: list[tuple[type[Any], Callable[..., Any]]] = [
            (Date, Date.parse_common_iso),
            (DateDelta, DateDelta.parse_common_iso),
            (DateTimeDelta, DateTimeDelta.parse_common_iso),
            (IPv4Address, IPv4Address),
            (IPv6Address, IPv6Address),
            (MonthDay, MonthDay.parse_common_iso),
            (Path, partial(_parse_path, resolve=resolve_paths, pwd=Path.cwd())),
            (PlainDateTime, PlainDateTime.parse_common_iso),
            (Time, Time.parse_common_iso),
            (TimeDelta, TimeDelta.parse_common_iso),
            (YearMonth, YearMonth.parse_common_iso),
            (ZonedDateTime, ZonedDateTime.parse_common_iso),
        ]
        extras = {cls: _make_converter(cls, func) for cls, func in cases}
        self.scalar_converters |= extras


def _make_converter[T](
    cls: type[T], parser: Callable[[str], T], /
) -> Callable[[Any, type[Any]], Any]:
    def hook(value: T | str, _: type[T] = cls, /) -> Any:
        if not isinstance(value, (cls, str)):  # pragma: no cover
            msg = f"Invalid type {type(value).__name__!r}; expected '{cls.__name__}' or 'str'"
            raise TypeError(msg)
        if isinstance(value, str):
            return parser(value)
        return value

    return hook


def _parse_path(
    path: str, /, *, resolve: bool = False, pwd: MaybeCallablePathLike | None = None
) -> Path:
    path = substitute_environ(path, **environ)
    match resolve:
        case True:
            return get_path(path=pwd).joinpath(path).resolve()
        case False:
            return Path(path)
        case _ as never:
            assert_never(never)


##


_BASE_DIR: Path = Path()


def load_settings[T](
    cls: type[T],
    app_name: str,
    /,
    *,
    filenames: MaybeIterable[str] = "settings.toml",
    start_dir: PathLike | None = None,
    loaders: MaybeIterable[Loader] | None = None,
    processors: MaybeIterable[Processor] = (),
    base_dir: Path = _BASE_DIR,
) -> T:
    if not search(r"^[A-Za-z]+(?:_[A-Za-z]+)*$", app_name):
        raise LoadSettingsError(appname=app_name)
    filenames_use = list(always_iterable(filenames))
    start_dir_use = None if start_dir is None else Path(start_dir)
    files = [find(filename, start_dir=start_dir_use) for filename in filenames_use]
    file_loader = FileLoader(formats={"*.toml": TomlFormat(app_name)}, files=files)
    env_loader = EnvLoader(f"{app_name.upper()}__", nested_delimiter="__")
    loaders_use: list[Loader] = [file_loader, env_loader]
    if loaders is not None:
        loaders_use.extend(always_iterable(loaders))
    return typed_settings.load_settings(
        cls,
        loaders_use,
        processors=list(always_iterable(processors)),
        converter=ExtendedTSConverter(),
        base_dir=base_dir,
    )


@dataclass(kw_only=True, slots=True)
class LoadSettingsError(Exception):
    appname: str

    @override
    def __str__(self) -> str:
        return f"Invalid app name; got {self.appname!r}"


__all__ = ["ExtendedTSConverter", "LoadSettingsError", "load_settings"]
