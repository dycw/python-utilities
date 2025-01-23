from __future__ import annotations

from typing import TYPE_CHECKING, Literal

from hypothesis import HealthCheck, given, settings
from hypothesis.strategies import sampled_from
from polars import int_range

from tests.test_operator import DataClass1, DataClass2Inner, DataClass2Outer, DataClass3
from utilities.pytest_regressions import (
    PolarsDataFrameRegressionFixture,
    orjson_regression,
    polars_dataframe_regression,
)

if TYPE_CHECKING:
    from utilities.pytest_regressions import OrjsonRegressionFixture


_ = orjson_regression
_ = polars_dataframe_regression


class TestPolarsDataFrameRegressionFixture:
    def test_main(
        self, *, polars_dataframe_regression: PolarsDataFrameRegressionFixture
    ) -> None:
        df = int_range(end=10, eager=True).alias("value").to_frame()
        polars_dataframe_regression.check(df)


class TestOrjsonRegressionFixture:
    def test_dataclass1(self, *, orjson_regression: OrjsonRegressionFixture) -> None:
        obj = DataClass1(x=0)
        orjson_regression.check(obj)

    def test_dataclass2(self, *, orjson_regression: OrjsonRegressionFixture) -> None:
        obj = DataClass2Outer(inner=DataClass2Inner(x=0))
        orjson_regression.check(obj)

    @given(truth=sampled_from(["true", "false"]))
    @settings(suppress_health_check={HealthCheck.function_scoped_fixture})
    def test_dataclass3(
        self,
        *,
        truth: Literal["true", "false"],
        orjson_regression: OrjsonRegressionFixture,
    ) -> None:
        obj = DataClass3(truth=truth)
        orjson_regression.check(obj, suffix=truth)
