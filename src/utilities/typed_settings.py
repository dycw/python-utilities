import datetime as dt
from collections.abc import Iterable
from typing import Any
from typing import TypeVar

from beartype import beartype
from cattrs import BaseConverter
from cattrs import Converter
from typed_settings import default_converter
from typed_settings import default_loaders
from typed_settings import load_settings as _load_settings

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
def _make_converter() -> BaseConverter | Converter:
    """Extend the default converter."""

    converter = default_converter()
    converter.register_structure_hook(dt.date, _to_date)
    converter.register_structure_hook(dt.time, _to_time)
    converter.register_structure_hook(dt.timedelta, _to_timedelta)
    return converter


@beartype
def _to_date(value: Any, _: type = dt.date) -> dt.date:
    """Convert a value to a date."""

    if isinstance(value, dt.date):
        return value
    elif isinstance(value, str):
        return dt.date.fromisoformat(value)
    else:
        raise TypeError(type(value))


@beartype
def _to_time(value: Any, _: type = dt.time) -> dt.time:
    """Convert a value to a time."""

    if isinstance(value, dt.time):
        return value
    elif isinstance(value, str):
        return dt.time.fromisoformat(value)
    else:
        raise TypeError(type(value))


@beartype
def _to_timedelta(value: Any, _: type = dt.timedelta) -> dt.timedelta:
    """Convert a value to a timedelta."""

    if isinstance(value, dt.timedelta):
        return value
    elif isinstance(value, str):
        as_date = dt.datetime.strptime(value, "%H:%M:%S")
        return dt.timedelta(
            hours=as_date.hour, minutes=as_date.minute, seconds=as_date.second
        )
    else:
        raise TypeError(type(value))
