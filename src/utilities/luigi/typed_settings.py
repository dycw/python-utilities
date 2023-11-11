from __future__ import annotations

import datetime as dt
from collections.abc import Callable
from contextlib import suppress
from dataclasses import asdict, fields
from enum import Enum
from functools import partial
from pathlib import Path
from types import UnionType, new_class
from typing import Any, Literal, TypeVar, cast, get_args, get_origin

from luigi import (
    BoolParameter,
    FloatParameter,
    IntParameter,
    ListParameter,
    OptionalBoolParameter,
    OptionalFloatParameter,
    OptionalIntParameter,
    OptionalListParameter,
    OptionalPathParameter,
    OptionalStrParameter,
    Parameter,
    PathParameter,
)
from typing_extensions import assert_never

from utilities.class_name import get_class_name
from utilities.luigi import (
    DateHourParameter,
    DateMinuteParameter,
    DateParameter,
    DateSecondParameter,
    EnumParameter,
    TimeParameter,
    WeekdayParameter,
)
from utilities.types import NoneType

_T = TypeVar("_T")


def build_params_mixin(obj: _T, /, **kwargs: Any) -> type[_T]:
    """Build a mixin of parameters for use in a `Task`."""
    mapping = asdict(obj)  # type: ignore

    def exec_body(namespace: dict[str, Any], /) -> None:
        for field in fields(type(obj)):
            key = field.name
            ann = field.type
            try:
                value = kwargs[key]
            except KeyError:
                kwargs_ann = {}
            else:
                kwargs_ann = _map_keywords(ann, value)
            param_cls = _map_annotation(ann, **kwargs_ann)
            namespace[key] = param_cls(default=mapping[key], positional=False)

    name = get_class_name(obj)
    return cast(type[_T], new_class(f"{name}Params", exec_body=exec_body))


def _map_keywords(ann: Any, kwargs: Any, /) -> dict[str, Any]:
    """Map an annotation and a set of keywords to a dictionary."""
    msg = f"{ann=}, {kwargs=}"
    if not isinstance(ann, type):
        raise InvalidAnnotationAndKeywordsError(msg)
    if issubclass(ann, dt.datetime):
        allowed = {"hour", "minute", "second"}
        if isinstance(kwargs, str) and (kwargs in allowed):
            return {"datetime": kwargs}
        if isinstance(kwargs, tuple):
            try:
                datetime, interval = kwargs
            except ValueError:
                raise InvalidAnnotationAndKeywordsError(msg) from None
            if (
                isinstance(datetime, str)
                and (datetime in allowed)
                and isinstance(interval, int)
            ):
                return {"datetime": datetime, "interval": interval}
        raise InvalidAnnotationAndKeywordsError(msg)
    if (
        issubclass(ann, dt.date)
        and isinstance(kwargs, str)
        and (kwargs in {"date", "weekday"})
    ):
        return {"date": kwargs}
    raise InvalidAnnotationAndKeywordsError(msg)


class InvalidAnnotationAndKeywordsError(Exception):
    """Raised when an (annotation, keywords) pair is invalid."""


def _map_annotation(  # noqa: PLR0911, PLR0912
    ann: Any,
    /,
    *,
    date: Literal["date", "weekday"] | None = None,
    datetime: Literal["hour", "minute", "second"] | None = None,
    interval: int = 1,
) -> type[Parameter] | Callable[..., Parameter]:
    """Map an annotation to a parameter class."""
    with suppress(InvalidAnnotationError):
        return _map_iterable_annotation(ann)
    with suppress(InvalidAnnotationError):
        return _map_union_annotation(ann)
    msg = f"{ann=}"
    if not isinstance(ann, type):
        raise InvalidAnnotationError(msg) from None
    if issubclass(ann, bool):
        return BoolParameter
    if issubclass(ann, dt.datetime):
        if datetime is None:
            raise AmbiguousDatetimeError
        return _map_datetime_annotation(kind=datetime, interval=interval)
    if issubclass(ann, dt.date):
        if date is None:
            raise AmbiguousDateError
        return _map_date_annotation(kind=date)
    if issubclass(ann, dt.time):
        return TimeParameter
    if issubclass(ann, Enum):
        return partial(EnumParameter, ann)
    if issubclass(ann, float):
        return FloatParameter
    if issubclass(ann, int):
        return IntParameter
    if issubclass(ann, Path):
        return PathParameter
    if issubclass(ann, str):
        return Parameter
    try:
        from sqlalchemy import Engine

        from utilities.luigi import EngineParameter
    except ModuleNotFoundError:  # pragma: no cover
        pass
    else:
        if issubclass(ann, Engine):
            return EngineParameter
    raise InvalidAnnotationError(msg)


class InvalidAnnotationError(Exception):
    """Raised when an annotation is invalid."""


def _map_iterable_annotation(ann: Any, /) -> type[ListParameter]:
    """Map an iterable annotation to a parameter class."""
    if get_origin(ann) in {frozenset, list, set}:
        return ListParameter
    msg = f"{ann=}"
    raise InvalidAnnotationError(msg)


def _map_union_annotation(ann: Any, /) -> type[Parameter] | Callable[..., Parameter]:
    """Map a union annotation to a parameter class."""
    msg = f"{ann=}"
    if get_origin(ann) is not UnionType:
        raise InvalidAnnotationError(msg)
    args = [arg for arg in get_args(ann) if arg is not NoneType]
    try:
        (arg,) = args
    except ValueError:
        raise InvalidAnnotationError(msg) from None
    if (inner := _map_annotation(arg)) is BoolParameter:
        return OptionalBoolParameter
    if inner is FloatParameter:
        return OptionalFloatParameter
    if inner is IntParameter:
        return OptionalIntParameter
    if inner is ListParameter:
        return OptionalListParameter
    if inner is PathParameter:
        return OptionalPathParameter
    if inner is Parameter:
        return OptionalStrParameter
    raise InvalidAnnotationError(msg)


def _map_date_annotation(
    *, kind: Literal["date", "weekday"]
) -> type[Parameter] | Callable[..., Parameter]:
    """Map a date annotation to a parameter class."""
    match kind:
        case "date":
            return DateParameter
        case "weekday":
            return WeekdayParameter
        case _:  # pragma: no cover  # type: ignore
            assert_never(kind)


class AmbiguousDateError(Exception):
    """Raised when a date is ambiguous."""


def _map_datetime_annotation(
    *, kind: Literal["hour", "minute", "second"], interval: int = 1
) -> type[Parameter] | Callable[..., Parameter]:
    """Map a datetime annotation to a parameter class."""
    match kind:
        case "hour":
            cls = DateHourParameter
        case "minute":
            cls = DateMinuteParameter
        case "second":
            cls = DateSecondParameter
        case _:  # pragma: no cover  # type: ignore
            assert_never(kind)
    return partial(cls, interval=interval)


class AmbiguousDatetimeError(Exception):
    """Raised when a datetime is ambiguous."""


__all__ = [
    "AmbiguousDateError",
    "AmbiguousDatetimeError",
    "build_params_mixin",
    "InvalidAnnotationAndKeywordsError",
    "InvalidAnnotationError",
]
