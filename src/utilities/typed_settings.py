import datetime as dt
from collections.abc import Callable, Iterable
from itertools import starmap
from operator import itemgetter
from typing import Any, Optional, TypeVar, Union, cast

from beartype import beartype
from cattrs import BaseConverter, Converter
from click import ParamType
from typed_settings import default_converter, default_loaders
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

from utilities.click import Date, DateTime, Time, Timedelta
from utilities.datetime import (
    ensure_date,
    ensure_datetime,
    ensure_time,
    ensure_timedelta,
    serialize_date,
    serialize_datetime,
    serialize_time,
)
from utilities.pathlib import PathLike

_T = TypeVar("_T")


@beartype
def load_settings(
    cls: type[_T],
    /,
    *,
    appname: str = "appname",
    config_files: Iterable[PathLike] = (),
) -> _T:
    """Load a settings object with the extended converter."""
    loaders = default_loaders(appname, config_files=config_files)
    converter = _make_converter()
    return _load_settings(cast(Any, cls), loaders, converter=converter)


@beartype
def click_options(
    cls: type[Any],
    /,
    *,
    appname: str = "appname",
    config_files: Iterable[PathLike] = (),
    argname: Optional[str] = None,
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Generate click options with the extended converter."""
    loaders = default_loaders(appname, config_files=config_files)
    converter = _make_converter()
    type_args_maker = TypeArgsMaker(cast(TypeHandler, _make_click_handler()))
    return _click_options(
        cls,
        loaders,
        converter=converter,
        type_args_maker=type_args_maker,
        argname=argname,
    )


@beartype
def _make_converter() -> Union[BaseConverter, Converter]:
    """Extend the default converter."""
    converter = default_converter()
    cases = [
        (dt.datetime, ensure_datetime),
        (dt.date, ensure_date),
        (dt.time, ensure_time),
        (dt.timedelta, ensure_timedelta),
    ]
    for cls, func in cases:
        hook = _make_structure_hook(cls, func)
        converter.register_structure_hook(cls, hook)
    return converter


@beartype
def _make_structure_hook(
    cls: type[Any],
    func: Callable[[Any], Any],
    /,
) -> Callable[[Any, type[Any]], Any]:
    """Make the structure hook for a given type."""

    @beartype
    def hook(value: Any, _: type[Any] = Any, /) -> Any:
        if not isinstance(value, (cls, str)):
            msg = f"Invalid type: {value=}"
            raise TypeError(msg)
        return func(value)

    return hook


@beartype
def _make_click_handler() -> ClickHandler:
    """Make the click handler."""
    cases = [
        (dt.datetime, DateTime, serialize_datetime),
        (dt.date, Date, serialize_date),
        (dt.time, Time, serialize_time),
        (dt.timedelta, Timedelta, str),
    ]
    extra_types = cast(
        dict[type, TypeHandlerFunc],
        dict(
            zip(
                map(itemgetter(0), cases),
                starmap(_make_type_handler_func, cases),
            ),
        ),
    )
    return ClickHandler(extra_types=extra_types)


@beartype
def _make_type_handler_func(
    cls: type[Any],
    param: type[ParamType],
    serialize: Callable[[Any], str],
    /,
) -> Callable[[Any, Any, Any], StrDict]:
    """Make the type handler for a given type/parameter."""

    @beartype
    def handler(
        _: type[Any],
        default: Default,
        is_optional: bool,  # noqa: FBT001
        /,
    ) -> StrDict:
        mapping: StrDict = {"type": param()}
        if isinstance(default, cls):  # pragma: no cover
            mapping["default"] = serialize(default)
        elif is_optional:  # pragma: no cover
            mapping["default"] = None
        return mapping

    return handler
