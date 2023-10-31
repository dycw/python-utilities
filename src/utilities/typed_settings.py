from __future__ import annotations

import datetime as dt
from collections.abc import Callable, Iterable
from enum import Enum
from itertools import starmap
from operator import attrgetter, itemgetter
from pathlib import Path
from re import search
from typing import Any, TypeVar, cast

from click import ParamType
from typed_settings import default_loaders
from typed_settings import load_settings as _load_settings
from typed_settings.cli_utils import (
    Default,
    StrDict,
    TypeArgsMaker,
    TypeHandler,
    TypeHandlerFunc,
)
from typed_settings.click_utils import ClickHandler
from typed_settings.click_utils import click_options as _click_options
from typed_settings.converters import TSConverter
from typed_settings.loaders import Loader
from typed_settings.types import AUTO, _Auto

from utilities.click import Date, DateTime, Time, Timedelta
from utilities.click import Enum as ClickEnum
from utilities.datetime import (
    ensure_date,
    ensure_time,
    ensure_timedelta,
    serialize_date,
    serialize_datetime,
    serialize_time,
)
from utilities.git import InvalidRepoError, get_repo_root
from utilities.pathlib import PathLike

_T = TypeVar("_T")


def get_repo_root_config(
    *, cwd: PathLike = Path.cwd(), filename: str = "config.toml"
) -> Path | None:
    """Get the config under the repo root, if it exists."""
    try:
        root = get_repo_root(cwd=cwd)
    except (FileNotFoundError, InvalidRepoError):
        return None
    if (path := root.joinpath(filename)).exists():
        return path
    return None


_CONFIG_FILES = [p for p in [get_repo_root_config()] if p is not None]


def load_settings(
    cls: type[_T],
    /,
    *,
    appname: str = "appname",
    config_files: Iterable[PathLike] = _CONFIG_FILES,
    config_file_section: str | _Auto = AUTO,
    config_files_var: None | str | _Auto = AUTO,
    env_prefix: None | str | _Auto = AUTO,
) -> _T:
    """Load a settings object with the extended converter."""
    loaders = _get_loaders(
        appname=appname,
        config_files=config_files,
        config_file_section=config_file_section,
        config_files_var=config_files_var,
        env_prefix=env_prefix,
    )
    converter = ExtendedTSConverter()
    return _load_settings(cast(Any, cls), loaders, converter=converter)


def click_options(
    cls: type[Any],
    /,
    *,
    appname: str = "appname",
    config_files: Iterable[PathLike] = _CONFIG_FILES,
    argname: str | None = None,
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Generate click options with the extended converter."""
    loaders = _get_loaders(appname=appname, config_files=config_files)
    converter = ExtendedTSConverter()
    type_args_maker = TypeArgsMaker(cast(TypeHandler, _make_click_handler()))
    return _click_options(
        cls,
        loaders,
        converter=converter,
        type_args_maker=type_args_maker,
        argname=argname,
    )


def _get_loaders(
    *,
    appname: str = "appname",
    config_files: Iterable[PathLike] = _CONFIG_FILES,
    config_file_section: str | _Auto = AUTO,
    config_files_var: None | str | _Auto = AUTO,
    env_prefix: None | str | _Auto = AUTO,
) -> list[Loader]:
    if search("_", appname):
        msg = f"{appname=}"
        raise AppNameContainsUnderscoreError(msg)
    return default_loaders(
        appname,
        config_files=config_files,
        config_file_section=config_file_section,
        config_files_var=config_files_var,
        env_prefix=env_prefix,
    )


class AppNameContainsUnderscoreError(ValueError):
    """Raised when the appname contains a space."""


class ExtendedTSConverter(TSConverter):
    def __init__(
        self,
        *,
        resolve_paths: bool = True,
        strlist_sep: str | Callable[[str], list] | None = ":",
    ) -> None:
        super().__init__(resolve_paths=resolve_paths, strlist_sep=strlist_sep)
        cases: list[tuple[type[Any], Callable[..., Any]]] = [
            (dt.date, ensure_date),
            (dt.time, ensure_time),
            (dt.timedelta, ensure_timedelta),
        ]
        try:
            from sqlalchemy import Engine

            from utilities.sqlalchemy.sqlalchemy import ensure_engine
        except ModuleNotFoundError:  # pragma: no cover
            pass
        else:
            cases.append((Engine, ensure_engine))
        extras = {cls: _make_structure_hook(cls, func) for cls, func in cases}
        self.scalar_converters |= extras


def _make_structure_hook(
    cls: type[Any], func: Callable[[Any], Any], /
) -> Callable[[Any, type[Any]], Any]:
    """Make the structure hook for a given type."""

    def hook(value: Any, _: type[Any] = Any, /) -> Any:
        if not isinstance(value, cls | str):
            msg = f"Could not convert value to {cls.__name__}: {value}"
            raise TypeError(msg)
        return func(value)

    return hook


def _make_click_handler() -> ClickHandler:
    """Make the click handler."""
    cases: list[tuple[type[Any], type[ParamType], Callable[[Any], str]]] = [
        (dt.datetime, DateTime, serialize_datetime),
        (dt.date, Date, serialize_date),
        (dt.time, Time, serialize_time),
        (dt.timedelta, Timedelta, str),
        (Enum, ClickEnum, attrgetter("name")),
    ]
    try:
        from sqlalchemy import Engine

        from utilities.click import Engine as ClickEngine
        from utilities.sqlalchemy import serialize_engine
    except ModuleNotFoundError:  # pragma: no cover
        pass
    else:
        cases.append((Engine, ClickEngine, serialize_engine))
    extra_types = dict(
        zip(
            map(itemgetter(0), cases),
            starmap(_make_type_handler_func, cases),
            strict=True,
        )
    )
    return ClickHandler(extra_types=extra_types)


def _make_type_handler_func(
    cls: type[Any], param: type[ParamType], serialize: Callable[[Any], str], /
) -> TypeHandlerFunc:
    """Make the type handler for a given type/parameter."""

    def handler(
        type_: type[Any],
        default: Default,
        is_optional: bool,  # noqa: FBT001
        /,
    ) -> StrDict:
        args = (type_,) if issubclass(type_, Enum) else ()
        mapping: StrDict = {"type": param(*args)}
        if isinstance(default, cls):  # pragma: no cover
            mapping["default"] = serialize(default)
        elif is_optional:  # pragma: no cover
            mapping["default"] = None
        return mapping

    return cast(TypeHandlerFunc, handler)
