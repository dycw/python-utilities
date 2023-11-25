import datetime as dt
from dataclasses import dataclass
from enum import Enum, auto
from pathlib import Path
from types import NoneType
from typing import Any, Literal

from hypothesis import given
from hypothesis.strategies import integers
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
    Task,
)
from pytest import mark, param, raises

from utilities._luigi.typed_settings import (
    AnnotationAndKeywordsToDictError,
    AnnotationIterableToClassError,
    AnnotationToClassError,
    AnnotationUnionToClassError,
    annotation_and_keywords_to_dict,
    annotation_date_to_class,
    annotation_datetime_to_class,
    annotation_iterable_to_class,
    annotation_to_class,
    annotation_union_to_class,
)
from utilities.datetime import TODAY_UTC
from utilities.hypothesis import namespace_mixins
from utilities.luigi import (
    DateHourParameter,
    DateMinuteParameter,
    DateParameter,
    DateSecondParameter,
    EnumParameter,
    TimeParameter,
    WeekdayParameter,
    build_params_mixin,
)


class TestAnnotationAndKeywordsToDict:
    @mark.parametrize("kind", [param("date"), param("weekday")])
    def test_date(self, *, kind: str) -> None:
        result = annotation_and_keywords_to_dict(dt.date, kind)
        expected = {"date": kind}
        assert result == expected

    @mark.parametrize("kind", [param("hour"), param("minute"), param("second")])
    def test_datetime_kind_only(self, *, kind: str) -> None:
        result = annotation_and_keywords_to_dict(dt.datetime, kind)
        expected = {"datetime": kind}
        assert result == expected

    @given(interval=integers(1, 10))
    @mark.parametrize("kind", [param("hour"), param("minute"), param("second")])
    def test_datetime_kind_and_interval(self, *, interval: int, kind: str) -> None:
        result = annotation_and_keywords_to_dict(dt.datetime, (kind, interval))
        expected = {"datetime": kind, "interval": interval}
        assert result == expected

    @mark.parametrize(
        ("ann", "kwargs"),
        [
            param(None, None),
            param(bool, None),
            param(dt.date, "invalid"),
            param(dt.datetime, "invalid"),
            param(dt.datetime, (0,)),
            param(dt.datetime, (0, 1)),
            param(dt.datetime, (0, 1, 2)),
        ],
    )
    def test_error(self, *, ann: Any, kwargs: Any) -> None:
        with raises(AnnotationAndKeywordsToDictError):
            _ = annotation_and_keywords_to_dict(ann, kwargs)


class TestAnnotationToClass:
    @mark.parametrize(
        ("ann", "expected"),
        [
            param(bool, BoolParameter),
            param(dt.time, TimeParameter),
            param(float, FloatParameter),
            param(int, IntParameter),
            param(Path, PathParameter),
            param(str, Parameter),
            param(frozenset[bool], ListParameter),
            param(list[bool], ListParameter),
            param(set[bool], ListParameter),
            param(bool | None, OptionalBoolParameter),
            param(frozenset[bool] | None, OptionalListParameter),
            param(list[bool] | None, OptionalListParameter),
            param(set[bool] | None, OptionalListParameter),
        ],
    )
    def test_main(self, *, ann: Any, expected: type[Parameter]) -> None:
        result = annotation_to_class(ann)
        param = result()
        assert isinstance(param, expected)

    @mark.parametrize("kind", [param("date"), param("weekday")])
    def test_date_success(self, *, kind: Literal["date", "weekday"]) -> None:
        _ = annotation_to_class(dt.date, date=kind)

    def test_date_error(self) -> None:
        with raises(AnnotationToClassError):
            _ = annotation_to_class(dt.date)

    @mark.parametrize("kind", [param("hour"), param("minute"), param("second")])
    def test_datetime_success(self, kind: Literal["hour", "minute", "second"]) -> None:
        _ = annotation_to_class(dt.datetime, datetime=kind)

    def test_datetime_error(self) -> None:
        with raises(AnnotationToClassError):
            _ = annotation_to_class(dt.datetime)

    def test_enum(self) -> None:
        class Example(Enum):
            member = auto()

        result = annotation_to_class(Example)
        param = result()
        assert isinstance(param, EnumParameter)
        assert param._enum is Example  # noqa: SLF001

    @mark.parametrize("ann", [param(None), param(NoneType)])
    def test_error(self, *, ann: Any) -> None:
        with raises(AnnotationToClassError):
            _ = annotation_to_class(ann)


class TestAnnotationDateToClass:
    @mark.parametrize(
        ("kind", "expected"),
        [param("date", DateParameter), param("weekday", WeekdayParameter)],
    )
    def test_main(
        self, *, kind: Literal["date", "weekday"], expected: type[Parameter]
    ) -> None:
        result = annotation_date_to_class(kind)
        param = result()
        assert isinstance(param, expected)


class TestAnnotationDatetimeToClass:
    @given(interval=integers(1, 10))
    @mark.parametrize(
        ("kind", "expected"),
        [
            param("hour", DateHourParameter),
            param("minute", DateMinuteParameter),
            param("second", DateSecondParameter),
        ],
    )
    def test_main(
        self,
        *,
        kind: Literal["hour", "minute", "second"],
        interval: int,
        expected: type[Parameter],
    ) -> None:
        result = annotation_datetime_to_class(kind, interval=interval)
        param = result()
        assert isinstance(param, expected)


class TestAnnotationIterableToClass:
    @mark.parametrize(
        "ann", [param(frozenset[bool]), param(list[bool]), param(set[bool])]
    )
    def test_main(self, *, ann: Any) -> None:
        assert annotation_iterable_to_class(ann) is ListParameter

    @mark.parametrize("ann", [param(None), param(bool), param(bool | None)])
    def test_error(self, *, ann: Any) -> None:
        with raises(AnnotationIterableToClassError):
            _ = annotation_iterable_to_class(ann)


class TestAnnotationUnionToClass:
    @mark.parametrize(
        ("ann", "expected"),
        [
            param(bool | None, OptionalBoolParameter),
            param(float | None, OptionalFloatParameter),
            param(Path | None, OptionalPathParameter),
            param(int | None, OptionalIntParameter),
            param(str | None, OptionalStrParameter),
            param(list[bool] | None, OptionalListParameter),
        ],
    )
    def test_main(self, *, ann: Any, expected: type[Parameter]) -> None:
        result = annotation_union_to_class(ann)
        param = result()
        assert isinstance(param, expected)

    @mark.parametrize("ann", [param(list[int]), param(int | float)])
    def test_errors(self, *, ann: Any) -> None:
        with raises(AnnotationUnionToClassError):
            _ = annotation_union_to_class(ann)

    def test_error_invalid_inner(self) -> None:
        with raises(AnnotationUnionToClassError):
            _ = annotation_union_to_class(dt.time | None)


class TestBuildParamsMixin:
    @given(namespace_mixin=namespace_mixins())
    def test_no_field(self, *, namespace_mixin: Any) -> None:
        @dataclass(frozen=True)
        class Config:
            value: int = 0

        config = Config()
        Params = build_params_mixin(config)  # noqa: N806

        class Example(namespace_mixin, Params, Task):
            pass

        task = Example()
        assert task.value == 0

    @given(namespace_mixin=namespace_mixins())
    def test_with_field(self, *, namespace_mixin: Any) -> None:
        @dataclass(frozen=True)
        class Config:
            date: dt.date = TODAY_UTC

        config = Config()
        Params = build_params_mixin(config, date="date")  # noqa: N806

        class Example(namespace_mixin, Params, Task):
            pass

        task = Example()
        assert task.date == TODAY_UTC
