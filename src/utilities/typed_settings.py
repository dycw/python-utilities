import datetime as dt
from collections.abc import Callable
from collections.abc import Iterable
from itertools import starmap
from operator import itemgetter
from typing import Any
from typing import TypeVar
from typing import cast

from attr import NOTHING
from beartype import beartype
from cattrs import BaseConverter
from cattrs import Converter
from click import ParamType
from typed_settings import default_converter
from typed_settings import default_loaders
from typed_settings import load_settings as _load_settings
from typed_settings.click_utils import DEFAULT_TYPES
from typed_settings.click_utils import TypeHandler
from typed_settings.click_utils import click_options as _click_options

from utilities.click import Date
from utilities.click import DateTime
from utilities.click import Time
from utilities.click import Timedelta
from utilities.datetime import ensure_date
from utilities.datetime import ensure_datetime
from utilities.datetime import ensure_time
from utilities.datetime import ensure_timedelta
from utilities.datetime import serialize_date
from utilities.datetime import serialize_datetime
from utilities.datetime import serialize_time
from utilities.pathlib import PathLike


_T = TypeVar("_T")


@beartype
def load_settings(
    cls: type[_T], appname: str, /, *, config_files: Iterable[PathLike] = ()
) -> _T:
    """Load a settings object with the extended converter."""

    loaders = default_loaders(appname, config_files=config_files)
    converter = _make_converter()
    return _load_settings(cls, loaders, converter=converter)


@beartype
def click_options(
    cls: type[Any],
    appname: str,
    /,
    *,
    config_files: Iterable[PathLike] = (),
    argname: str | None = None,
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Generate click options with the extended converter."""

    loaders = default_loaders(appname, config_files=config_files)
    converter = _make_converter()
    type_handler = _make_type_handler()
    return _click_options(
        cls,
        loaders,
        converter=converter,
        type_handler=type_handler,
        argname=argname,
    )


@beartype
def _make_converter() -> BaseConverter | Converter:
    """Extend the default converter."""

    converter = default_converter()
    cases = [
        (dt.date, ensure_date),
        (dt.datetime, ensure_datetime),
        (dt.time, ensure_time),
        (dt.timedelta, ensure_timedelta),
    ]
    for cls, func in cases:
        hook = _make_structure_hook_for_class(cls, func)
        converter.register_structure_hook(cls, hook)
    return converter


@beartype
def _make_structure_hook_for_class(
    cls: type[Any], func: Callable[[Any], Any], /
) -> Callable[[Any, type[Any]], Any]:
    @beartype
    def hook(value: Any, _: type[Any] = Any, /) -> Any:
        if isinstance(value, (cls, str)):
            return func(value)
        else:
            raise TypeError(type(value))

    return hook


@beartype
def _make_type_handler() -> TypeHandler:
    cases = [
        (dt.date, Date, serialize_date),
        (dt.datetime, DateTime, serialize_datetime),
        (dt.time, Time, serialize_time),
        (dt.timedelta, Timedelta, str),
    ]
    extended = dict(
        zip(
            map(itemgetter(0), cases),
            starmap(_make_type_handler_for_class, cases),
        )
    )
    return TypeHandler(types=DEFAULT_TYPES | extended)


@beartype
def _make_type_handler_for_class(
    cls: type[Any], param: type[ParamType], serialize: Callable[[Any], str], /
) -> Callable[[Any, Any], dict[str, Any]]:
    @beartype
    def handler(_: type[Any], default: Any, /) -> dict[str, Any]:
        mapping = cast(dict[str, Any], {"type": param()})
        if default is not NOTHING and isinstance(  # pragma: no cover
            default, cls
        ):
            mapping["default"] = serialize(default)
        return mapping

    return handler
