from __future__ import annotations

import datetime as dt
import enum
from dataclasses import MISSING, dataclass, field
from itertools import starmap
from operator import attrgetter, itemgetter
from re import search
from typing import TYPE_CHECKING, Any, TypeVar, cast

from typed_settings import default_loaders
from typed_settings import load_settings as _load_settings
from typed_settings.cli_click import ClickHandler
from typed_settings.cli_click import click_options as _click_options
from typed_settings.cli_utils import (
    Default,
    StrDict,
    TypeArgsMaker,
    TypeHandler,
    TypeHandlerFunc,
)
from typed_settings.constants import CLICK_METADATA_KEY, METADATA_KEY
from typed_settings.converters import TSConverter
from typed_settings.types import AUTO, _Auto
from typing_extensions import override

import utilities.click
from utilities.click import Date, DateTime, Time, Timedelta
from utilities.datetime import (
    ensure_date,
    ensure_time,
    ensure_timedelta,
    serialize_date,
    serialize_datetime,
    serialize_time,
)
from utilities.git import get_repo_root_or_cwd_sub_path
from utilities.pathlib import ensure_path
from utilities.types import PathLike, ensure_class

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Mapping
    from pathlib import Path

    from click import ParamType
    from typed_settings.loaders import Loader

_T = TypeVar("_T")


def _config_toml(root: Path, /) -> Path | None:
    return path if (path := ensure_path(root, "config.toml")).exists() else None


_CONFIG_FILES = [
    p for p in [get_repo_root_or_cwd_sub_path(_config_toml)] if p is not None
]


class _ExtendedTSConverter(TSConverter):
    """An extension of the TSConverter for custom types."""

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

        extras = {cls: _pair_type_and_converter(cls, func) for cls, func in cases}
        self.scalar_converters |= extras


def _pair_type_and_converter(
    cls: type[Any], func: Callable[[Any], Any], /
) -> Callable[[Any, type[Any]], Any]:
    def hook(value: Any, _: type[Any] = Any, /) -> Any:
        return func(ensure_class(value, (cls, str)))

    return hook


def _get_loaders(
    *,
    appname: str = "appname",
    config_files: Iterable[PathLike] = _CONFIG_FILES,
    config_file_section: str | _Auto = AUTO,
    config_files_var: None | str | _Auto = AUTO,
    env_prefix: None | str | _Auto = AUTO,
) -> list[Loader]:
    if search("_", appname):
        raise _GetLoadersError(appname=appname)
    return default_loaders(
        appname,
        config_files=config_files,
        config_file_section=config_file_section,
        config_files_var=config_files_var,
        env_prefix=env_prefix,
    )


@dataclass(kw_only=True)
class _GetLoadersError(Exception):
    appname: str

    @override
    def __str__(self) -> str:
        return f"App name {self.appname!r} must not contain underscores"


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
    converter = _ExtendedTSConverter()
    type_args_maker = TypeArgsMaker(cast(TypeHandler, _make_click_handler()))
    return _click_options(
        cls,
        loaders,
        converter=converter,
        type_args_maker=type_args_maker,
        argname=argname,
    )


def _make_click_handler() -> ClickHandler:
    """Make the click handler."""
    cases: list[tuple[type[Any], type[ParamType], Callable[[Any], str]]] = [
        (dt.datetime, DateTime, serialize_datetime),
        (dt.date, Date, serialize_date),
        (dt.time, Time, serialize_time),
        (dt.timedelta, Timedelta, str),
        (enum.Enum, utilities.click.Enum, attrgetter("name")),
    ]
    try:
        import sqlalchemy

        from utilities.sqlalchemy import serialize_engine
    except ModuleNotFoundError:  # pragma: no cover
        pass
    else:
        cases.append((sqlalchemy.Engine, utilities.click.Engine, serialize_engine))
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
        args = (type_,) if issubclass(type_, enum.Enum) else ()
        mapping: StrDict = {"type": param(*args)}
        if isinstance(default, cls):  # pragma: no cover
            mapping["default"] = serialize(default)
        elif is_optional:  # pragma: no cover
            mapping["default"] = None
        return mapping

    return cast(TypeHandlerFunc, handler)


def click_field(
    *,
    default: Any = MISSING,
    init: bool = True,
    repr: bool = True,  # noqa: A002
    hash: bool | None = None,  # noqa: A002
    compare: bool = True,
    metadata: Mapping[str, Any] | None = None,
    kw_only: Any = MISSING,
    help: str | None = None,  # noqa: A002
    click: Mapping[str, Any] | None = None,
    param_decls: tuple[str, ...] | None = None,
) -> Any:
    """Create a click field."""
    click_use = ({} if click is None else dict(click)) | (
        {} if param_decls is None else {"param_decls": param_decls}
    )
    metadata_use = _get_metadata(metadata=metadata, help_=help, click=click_use)
    return field(
        default=default,
        init=init,
        repr=repr,
        hash=hash,
        compare=compare,
        metadata=metadata_use,
        kw_only=kw_only,
    )


def _get_metadata(
    *,
    metadata: Mapping[str, Any] | None = None,
    help_: str | None = None,
    click: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    # copied from typed_settings.cls_attrs, which we cannot import
    metadata_use = {} if metadata is None else dict(metadata)
    ts_meta = metadata_use.setdefault(METADATA_KEY, {})
    ts_meta["help"] = help_
    ts_meta[CLICK_METADATA_KEY] = {"help": help_} | (
        {} if click is None else dict(click)
    )
    return metadata_use


__all__ = ["click_field", "click_options", "load_settings"]
