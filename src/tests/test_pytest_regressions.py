from __future__ import annotations

from typing import TYPE_CHECKING

from tests.test_operator import DataClass1
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
