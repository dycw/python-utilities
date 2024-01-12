import datetime as dt
from collections.abc import Callable
from contextlib import suppress
from dataclasses import asdict, fields
from enum import Enum
from functools import partial
from pathlib import Path
from types import NoneType, UnionType, new_class
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

from utilities._luigi.common import (
    DateHourParameter,
    DateMinuteParameter,
    DateParameter,
    DateSecondParameter,
    EnumParameter,
    TimeParameter,
    WeekdayParameter,
)
from utilities.dataclasses import Dataclass
from utilities.errors import redirect_error
from utilities.types import get_class_name

_T = TypeVar("_T", bound=Dataclass)


def build_params_mixin(obj: _T, /, **kwargs: Any) -> type[_T]:
    """Build a mixin of parameters for use in a `Task`."""
    mapping = asdict(obj)

    def exec_body(namespace: dict[str, Any], /) -> None:
        for field in fields(type(obj)):
            key = field.name
            ann = field.type
            try:
                value = kwargs[key]
            except KeyError:
                kwargs_ann = {}
            else:
                kwargs_ann = annotation_and_keywords_to_dict(ann, value)
            param_cls = annotation_to_class(ann, **kwargs_ann)
            namespace[key] = param_cls(default=mapping[key], positional=False)

    name = get_class_name(obj)
    return cast(type[_T], new_class(f"{name}Params", exec_body=exec_body))


def annotation_and_keywords_to_dict(ann: Any, kwargs: Any, /) -> dict[str, Any]:
    """Map an annotation and a set of keywords to a dictionary."""
    if not isinstance(ann, type):
        msg = f"{ann=}"
        raise AnnotationAndKeywordsToDictError(msg)
    if issubclass(ann, dt.datetime):
        allowed = {"hour", "minute", "second"}
        if isinstance(kwargs, str) and (kwargs in allowed):
            return {"datetime": kwargs}
        if isinstance(kwargs, tuple):
            with redirect_error(
                ValueError, AnnotationAndKeywordsToDictError(f"{ann=}, {kwargs=}")
            ):
                datetime, interval = kwargs
            if (
                isinstance(datetime, str)
                and (datetime in allowed)
                and isinstance(interval, int)
            ):
                return {"datetime": datetime, "interval": interval}
        msg = f"{ann=}"
        raise AnnotationAndKeywordsToDictError(msg)
    if (
        issubclass(ann, dt.date)
        and isinstance(kwargs, str)
        and (kwargs in {"date", "weekday"})
    ):
        return {"date": kwargs}
    msg = f"{ann=}, {kwargs=}"
    raise AnnotationAndKeywordsToDictError(msg)


class AnnotationAndKeywordsToDictError(Exception):
    ...


def annotation_to_class(  # noqa: PLR0911, PLR0912
    ann: Any,
    /,
    *,
    date: Literal["date", "weekday"] | None = None,
    datetime: Literal["hour", "minute", "second"] | None = None,
    interval: int = 1,
) -> type[Parameter] | Callable[..., Parameter]:
    """Map an annotation to a parameter class."""
    with suppress(AnnotationIterableToClassError):
        msg = f"{ann=}"
        return annotation_iterable_to_class(ann)
    with suppress(AnnotationUnionToClassError):
        msg = f"{ann=}"
        return annotation_union_to_class(ann)
    if not isinstance(ann, type):
        msg = f"{ann=}"
        raise AnnotationToClassError(msg)
    if issubclass(ann, bool):
        return BoolParameter
    if issubclass(ann, dt.datetime):
        if datetime is None:
            msg = f"{ann=}, {datetime=}"
            raise AnnotationToClassError(msg)
        return annotation_datetime_to_class(datetime, interval=interval)
    if issubclass(ann, dt.date):
        if date is None:
            msg = f"{ann=}, {date=}"
            raise AnnotationToClassError(msg)
        return annotation_date_to_class(date)
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

        from utilities._luigi.sqlalchemy import EngineParameter
    except ModuleNotFoundError:  # pragma: no cover
        pass
    else:
        if issubclass(ann, Engine):
            return EngineParameter
    msg = f"{ann=}"
    raise AnnotationToClassError(msg)


class AnnotationToClassError(Exception):
    ...


def annotation_date_to_class(
    kind: Literal["date", "weekday"], /
) -> type[Parameter] | Callable[..., Parameter]:
    """Map a date annotation to a parameter class."""
    match kind:
        case "date":
            return DateParameter
        case "weekday":
            return WeekdayParameter
        case _ as never:  # type: ignore
            assert_never(never)


def annotation_datetime_to_class(
    kind: Literal["hour", "minute", "second"], /, *, interval: int = 1
) -> type[Parameter] | Callable[..., Parameter]:
    """Map a datetime annotation to a parameter class."""
    match kind:
        case "hour":
            cls = DateHourParameter
        case "minute":
            cls = DateMinuteParameter
        case "second":
            cls = DateSecondParameter
        case _ as never:  # type: ignore
            assert_never(never)
    return partial(cls, interval=interval)


def annotation_iterable_to_class(ann: Any, /) -> type[ListParameter]:
    """Map an iterable annotation to a parameter class."""
    if get_origin(ann) in {frozenset, list, set}:
        return ListParameter
    msg = f"{ann=}"
    raise AnnotationIterableToClassError(msg)


class AnnotationIterableToClassError(Exception):
    ...


def annotation_union_to_class(
    ann: Any, /
) -> type[Parameter] | Callable[..., Parameter]:
    """Map a union annotation to a parameter class."""
    if get_origin(ann) is not UnionType:
        msg = f"{ann=}"
        raise AnnotationUnionToClassError(msg)
    args = [arg for arg in get_args(ann) if arg is not NoneType]
    with redirect_error(ValueError, AnnotationUnionToClassError(f"{ann=}")):
        (arg,) = args
    if (inner := annotation_to_class(arg)) is BoolParameter:
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
    msg = f"{ann=}"
    raise AnnotationUnionToClassError(msg)


class AnnotationUnionToClassError(Exception):
    ...
