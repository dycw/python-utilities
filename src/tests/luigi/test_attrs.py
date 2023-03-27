import datetime as dt
from enum import Enum, auto
from pathlib import Path
from typing import Any, Literal, Optional, Union

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
from sqlalchemy import Engine
from typed_settings import settings

from utilities.datetime import TODAY
from utilities.hypothesis.luigi import namespace_mixins
from utilities.luigi import (
    DateHourParameter,
    DateMinuteParameter,
    DateParameter,
    DateSecondParameter,
    EnumParameter,
    TimeParameter,
    WeekdayParameter,
)
from utilities.luigi.attrs import (
    AmbiguousDateError,
    AmbiguousDatetimeError,
    InvalidAnnotationAndKeywordsError,
    InvalidAnnotationError,
    _map_annotation,
    _map_date_annotation,
    _map_datetime_annotation,
    _map_iterable_annotation,
    _map_keywords,
    _map_union_annotation,
    build_params_mixin,
)
from utilities.luigi.sqlalchemy import EngineParameter
from utilities.sentinel import Sentinel


class TestBuildParamsMixin:
    @given(namespace_mixin=namespace_mixins())
    def test_no_field(self, namespace_mixin: Any) -> None:
        @settings
        class Config:
            value: int = 0

        config = Config()
        Params = build_params_mixin(config)  # noqa: N806

        class Example(namespace_mixin, Params, Task):
            pass

        task = Example()
        assert task.value == 0

    @given(namespace_mixin=namespace_mixins())
    def test_with_field(self, namespace_mixin: Any) -> None:
        @settings
        class Config:
            date: dt.date = TODAY

        config = Config()
        Params = build_params_mixin(config, date="date")  # noqa: N806

        class Example(namespace_mixin, Params, Task):
            pass

        task = Example()
        assert task.date == TODAY


class TestMapAnnotation:
    @mark.parametrize(
        ("ann", "expected"),
        [
            param(bool, BoolParameter),
            param(Engine, EngineParameter),
            param(dt.time, TimeParameter),
            param(float, FloatParameter),
            param(int, IntParameter),
            param(Path, PathParameter),
            param(str, Parameter),
            param(frozenset[bool], ListParameter),
            param(list[bool], ListParameter),
            param(set[bool], ListParameter),
            param(Optional[bool], OptionalBoolParameter),
            param(Optional[frozenset[bool]], OptionalListParameter),
            param(Optional[list[bool]], OptionalListParameter),
            param(Optional[set[bool]], OptionalListParameter),
        ],
    )
    def test_main(self, ann: Any, expected: type[Parameter]) -> None:
        result = _map_annotation(ann)
        param = result()
        assert isinstance(param, expected)

    @mark.parametrize("kind", [param("date"), param("weekday")])
    def test_date_success(self, kind: Literal["date", "weekday"]) -> None:
        _ = _map_annotation(dt.date, date=kind)

    def test_date_error(self) -> None:
        with raises(AmbiguousDateError):
            _ = _map_annotation(dt.date)

    @mark.parametrize("kind", [param("hour"), param("minute"), param("second")])
    def test_datetime_success(self, kind: Literal["hour", "minute", "second"]) -> None:
        _ = _map_annotation(dt.datetime, datetime=kind)

    def test_datetime_error(self) -> None:
        with raises(AmbiguousDatetimeError):
            _ = _map_annotation(dt.datetime)

    def test_enum(self) -> None:
        class Example(Enum):
            member = auto()

        result = _map_annotation(Example)
        param = result()
        assert isinstance(param, EnumParameter)
        assert param._enum is Example  # noqa: SLF001

    @mark.parametrize("ann", [param(None), param(Sentinel)])
    def test_invalid(self, ann: Any) -> None:
        with raises(InvalidAnnotationError):
            _ = _map_annotation(ann)


class TestMapDateAnnotation:
    @mark.parametrize(
        ("kind", "expected"),
        [param("date", DateParameter), param("weekday", WeekdayParameter)],
    )
    def test_main(
        self, kind: Literal["date", "weekday"], expected: type[Parameter]
    ) -> None:
        result = _map_date_annotation(kind=kind)
        param = result()
        assert isinstance(param, expected)


class TestMapDatetimeAnnotation:
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
        kind: Literal["hour", "minute", "second"],
        interval: int,
        expected: type[Parameter],
    ) -> None:
        result = _map_datetime_annotation(kind=kind, interval=interval)
        param = result()
        assert isinstance(param, expected)


class TestMapIterableAnnotation:
    @mark.parametrize(
        "ann", [param(frozenset[bool]), param(list[bool]), param(set[bool])]
    )
    def test_main(self, ann: Any) -> None:
        assert _map_iterable_annotation(ann) is ListParameter

    @mark.parametrize("ann", [param(None), param(bool), param(Optional[bool])])
    def test_invalid(self, ann: Any) -> None:
        with raises(InvalidAnnotationError):
            _ = _map_iterable_annotation(ann)


class TestMapKeywords:
    @mark.parametrize("kind", [param("date"), param("weekday")])
    def test_date(self, kind: str) -> None:
        result = _map_keywords(dt.date, kind)
        expected = {"date": kind}
        assert result == expected

    @mark.parametrize("kind", [param("hour"), param("minute"), param("second")])
    def test_datetime_kind_only(self, kind: str) -> None:
        result = _map_keywords(dt.datetime, kind)
        expected = {"datetime": kind}
        assert result == expected

    @given(interval=integers(1, 10))
    @mark.parametrize("kind", [param("hour"), param("minute"), param("second")])
    def test_datetime_kind_and_interval(self, interval: int, kind: str) -> None:
        result = _map_keywords(dt.datetime, (kind, interval))
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
    def test_invalid(self, ann: Any, kwargs: Any) -> None:
        with raises(InvalidAnnotationAndKeywordsError):
            _ = _map_keywords(ann, kwargs)


class TestMapUnionAnnotation:
    @mark.parametrize(
        ("ann", "expected"),
        [
            param(Optional[bool], OptionalBoolParameter),
            param(Optional[float], OptionalFloatParameter),
            param(Optional[Path], OptionalPathParameter),
            param(Optional[int], OptionalIntParameter),
            param(Optional[str], OptionalStrParameter),
            param(Optional[list[bool]], OptionalListParameter),
        ],
    )
    def test_main(self, ann: Any, expected: type[Parameter]) -> None:
        result = _map_union_annotation(ann)
        param = result()
        assert isinstance(param, expected)

    @mark.parametrize(
        "ann", [param(list[bool]), param(Optional[Sentinel]), param(Union[int, float])]
    )
    def test_invalid(self, ann: Any) -> None:
        with raises(InvalidAnnotationError):
            _ = _map_union_annotation(ann)

    def test_invalid_enum(self) -> None:
        class Example(Enum):
            member = auto()

        with raises(InvalidAnnotationError):
            _ = _map_union_annotation(Optional[Example])
