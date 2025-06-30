from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pathlib import Path

    from pytest import FixtureRequest

    from utilities.pytest_regressions import (
        DataFrameRegressionFixture,
        OrjsonRegressionFixture,
        SeriesRegressionFixture,
    )


try:
    from pytest import fixture
except ModuleNotFoundError:
    pass
else:

    @fixture
    def orjson_regression(
        *, request: FixtureRequest, tmp_path: Path
    ) -> OrjsonRegressionFixture:
        """Instance of the `OrjsonRegressionFixture`."""
        from utilities.pytest_regressions import OrjsonRegressionFixture

        return OrjsonRegressionFixture(request=request, tmp_path=tmp_path)

    @fixture
    def df_regression(
        *, request: FixtureRequest, tmp_path: Path
    ) -> DataFrameRegressionFixture:
        """Instance of the `DataFrameRegressionFixture`."""
        from utilities.pytest_regressions import DataFrameRegressionFixture

        return DataFrameRegressionFixture(request=request, tmp_path=tmp_path)

    @fixture
    def series_regression(
        *, request: FixtureRequest, tmp_path: Path
    ) -> SeriesRegressionFixture:
        """Instance of the `SeriesRegressionFixture`."""
        from utilities.pytest_regressions import SeriesRegressionFixture

        return SeriesRegressionFixture(request=request, tmp_path=tmp_path)


__all__ = ["df_regression", "orjson_regression", "series_regression"]
