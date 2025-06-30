from __future__ import annotations

from typing import TYPE_CHECKING, Literal

from hypothesis import HealthCheck, Phase, given, reproduce_failure, settings
from hypothesis.strategies import sampled_from
from polars import int_range
from pytest import mark, param

from tests.test_typing_funcs.with_future import (
    DataClassFutureInt,
    DataClassFutureLiteral,
    DataClassFutureNestedOuterFirstInner,
    DataClassFutureNestedOuterFirstOuter,
)
from utilities.contextvars import set_global_breakpoint

if TYPE_CHECKING:
    from utilities.pytest_regressions import (
        OrjsonRegressionFixture,
        PolarsRegressionFixture,
    )


class TestMultipleRegressionFixtures:
    def test_main(
        self,
        *,
        orjson_regression: OrjsonRegressionFixture,
        polars_regression: PolarsRegressionFixture,
    ) -> None:
        obj = DataClassFutureInt(int_=0)
        orjson_regression.check(obj, suffix="obj")
        series = int_range(end=10, eager=True).alias("value")
        polars_regression.check(series, suffix="series")


class TestPolarsRegressionFixture:
    @mark.parametrize("summary", [param(True), param(False)])
    @mark.parametrize("compress", [param(True), param(False)])
    def test_dataframe(
        self,
        *,
        polars_regression: PolarsRegressionFixture,
        summary: bool,
        compress: bool,
    ) -> None:
        df = int_range(end=10, eager=True).alias("value").to_frame()
        polars_regression.check(df, summary=summary, compress=compress)

    @mark.parametrize("summary", [param(True), param(False)])
    @mark.parametrize("compress", [param(True), param(False)])
    def test_series(
        self,
        *,
        polars_regression: PolarsRegressionFixture,
        summary: bool,
        compress: bool,
    ) -> None:
        series = int_range(end=10, eager=True).alias("value")
        polars_regression.check(series, summary=summary, compress=compress)


class TestOrjsonRegressionFixture:
    @mark.parametrize("compress", [param(True, marks=mark.only), param(False)])
    def test_dataclass_nested(
        self, *, orjson_regression: OrjsonRegressionFixture, compress: bool
    ) -> None:
        obj = DataClassFutureNestedOuterFirstOuter(
            inner=DataClassFutureNestedOuterFirstInner(int_=0)
        )
        orjson_regression.check(obj, compress=compress)

    def test_dataclass_int(self, *, orjson_regression: OrjsonRegressionFixture) -> None:
        obj = DataClassFutureInt(int_=0)
        orjson_regression.check(obj)

    @mark.parametrize("truth", [param("true"), param("false")])
    @mark.parametrize("compress", [param(True), param(False)])
    @mark.only
    def test_dataclass_literal(
        self,
        *,
        truth: Literal["true", "false"],
        orjson_regression: OrjsonRegressionFixture,
        compress: bool,
    ) -> None:
        obj = DataClassFutureLiteral(truth=truth)
        orjson_regression.check(obj, compress=compress, suffix=truth)
