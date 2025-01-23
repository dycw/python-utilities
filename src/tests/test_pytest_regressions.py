from __future__ import annotations

from typing import TYPE_CHECKING, Literal

from hypothesis import HealthCheck, given, settings
from hypothesis.strategies import sampled_from

from tests.test_operator import DataClass1, DataClass2Inner, DataClass2Outer, DataClass3
from utilities.pytest_regressions import orjson_regression_fixture

if TYPE_CHECKING:
    from utilities.pytest_regressions import OrjsonRegressionFixture


_ = orjson_regression_fixture


class TestOrjsonRegressionFixture:
    def test_dataclass1(
        self, *, orjson_regression_fixture: OrjsonRegressionFixture
    ) -> None:
        obj = DataClass1(x=0)
        orjson_regression_fixture.check(obj)

    def test_dataclass2(
        self, *, orjson_regression_fixture: OrjsonRegressionFixture
    ) -> None:
        obj = DataClass2Outer(inner=DataClass2Inner(x=0))
        orjson_regression_fixture.check(obj)

    @given(truth=sampled_from(["true", "false"]))
    @settings(suppress_health_check={HealthCheck.function_scoped_fixture})
    def test_dataclass3(
        self,
        *,
        truth: Literal["true", "false"],
        orjson_regression_fixture: OrjsonRegressionFixture,
    ) -> None:
        obj = DataClass3(truth=truth)
        orjson_regression_fixture.check(obj, suffix=truth)
