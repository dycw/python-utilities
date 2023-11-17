from __future__ import annotations

import datetime as dt
from collections.abc import Callable, Iterable, Mapping
from dataclasses import MISSING, field
from enum import Enum
from itertools import starmap
from operator import attrgetter, itemgetter
from typing import Any, cast

from click import ParamType
from typed_settings.cli_utils import (
    Default,
    StrDict,
    TypeArgsMaker,
    TypeHandler,
    TypeHandlerFunc,
)
from typed_settings.click_utils import ClickHandler
from typed_settings.click_utils import click_options as _click_options
from typed_settings.constants import CLICK_METADATA_KEY, METADATA_KEY

from utilities.click import Date, DateTime, Time, Timedelta
from utilities.click import Enum as ClickEnum
from utilities.datetime import serialize_date, serialize_datetime, serialize_time
from utilities.pathlib import PathLike
from utilities.typed_settings.typed_settings import (
    _CONFIG_FILES,
    _ExtendedTSConverter,
    _get_loaders,
)


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


__all__ = ["click_field", "click_options"]
