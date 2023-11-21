from __future__ import annotations

import datetime as dt
from collections.abc import Callable, Iterable
from pathlib import Path
from re import search
from typing import Any, TypeVar, cast

from typed_settings import default_loaders
from typed_settings import load_settings as _load_settings
from typed_settings.converters import TSConverter
from typed_settings.loaders import Loader
from typed_settings.types import AUTO, _Auto

from utilities.datetime import ensure_date, ensure_time, ensure_timedelta
from utilities.git import get_repo_root_or_cwd_sub_path
from utilities.pathlib import PathLike

_T = TypeVar("_T")


def _config_toml(root: Path, /) -> Path | None:
    return path if (path := root.joinpath("config.toml")).exists() else None


_CONFIG_FILES = [
    p for p in [get_repo_root_or_cwd_sub_path(_config_toml)] if p is not None
]


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
    converter = _ExtendedTSConverter()
    return _load_settings(cast(Any, cls), loaders, converter=converter)


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


class AppNameContainsUnderscoreError(Exception):
    """Raised when the appname contains a space."""


class _ExtendedTSConverter(TSConverter):
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

            from utilities.sqlalchemy import ensure_engine
        except ModuleNotFoundError:  # pragma: no cover
            pass
        else:
            cases.append((Engine, ensure_engine))
        extras = {cls: _make_converter(cls, func) for cls, func in cases}
        self.scalar_converters |= extras


def _make_converter(
    cls: type[Any], func: Callable[[Any], Any], /
) -> Callable[[Any, type[Any]], Any]:
    """Lift a callable into a connverter."""

    def hook(value: Any, _: type[Any] = Any, /) -> Any:
        if not isinstance(value, cls | str):
            msg = f"Could not convert value to {cls.__name__}: {value}"
            raise TypeError(msg)
        return func(value)

    return hook


__all__ = ["AppNameContainsUnderscoreError", "load_settings"]
