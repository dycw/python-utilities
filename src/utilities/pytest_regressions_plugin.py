from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pytest import FixtureRequest

    from utilities.pytest_regressions import (
        OrjsonRegressionFixture,
        PolarsRegressionFixture,
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

        path = _get_path(request)
        return OrjsonRegressionFixture(path, request, tmp_path)

    @fixture
    def polars_regression(
        *, request: FixtureRequest, tmp_path: Path
    ) -> PolarsRegressionFixture:
        """Instance of the `PolarsRegressionFixture`."""
        from utilities.pytest_regressions import PolarsRegressionFixture

        path = _get_path(request)
        return PolarsRegressionFixture(path, request, tmp_path)


def _get_path(request: FixtureRequest, /) -> Path:
    from utilities.pathlib import get_root
    from utilities.pytest import node_id_to_path

    head = Path("src", "tests")
    tail = node_id_to_path(request.node.nodeid, head=head)
    return get_root().joinpath(head, "regressions", tail)


__all__ = ["orjson_regression", "polars_regression"]
