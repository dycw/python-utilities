from __future__ import annotations

from typing import TYPE_CHECKING, Literal

from hypothesis import HealthCheck, given, settings
from hypothesis.strategies import sampled_from
from polars import int_range

from tests.test_typing_funcs.with_future import (
    DataClassFutureInt,
    DataClassFutureLiteral,
    DataClassFutureNestedOuterFirstInner,
    DataClassFutureNestedOuterFirstOuter,
)

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
    def test_dataframe(self, *, polars_regression: PolarsRegressionFixture) -> None:
        df = int_range(end=10, eager=True).alias("value").to_frame()
        polars_regression.check(df)

    def test_series(self, *, polars_regression: PolarsRegressionFixture) -> None:
        series = int_range(end=10, eager=True).alias("value")
        polars_regression.check(series)


class TestOrjsonRegressionFixture:
    def test_dataclass_nested(
        self, *, orjson_regression: OrjsonRegressionFixture
    ) -> None:
        obj = DataClassFutureNestedOuterFirstOuter(
            inner=DataClassFutureNestedOuterFirstInner(int_=0)
        )
        orjson_regression.check(obj)

    def test_dataclass_int(self, *, orjson_regression: OrjsonRegressionFixture) -> None:
        obj = DataClassFutureInt(int_=0)
        orjson_regression.check(obj)

    @given(truth=sampled_from(["true", "false"]))
    @settings(suppress_health_check={HealthCheck.function_scoped_fixture})
    def test_dataclass_literal(
        self,
        *,
        truth: Literal["true", "false"],
        orjson_regression: OrjsonRegressionFixture,
    ) -> None:
        obj = DataClassFutureLiteral(truth=truth)
        orjson_regression.check(obj, suffix=truth)
