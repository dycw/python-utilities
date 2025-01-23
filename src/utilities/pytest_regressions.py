from __future__ import annotations

from contextlib import suppress
from json import loads
from pathlib import Path
from typing import TYPE_CHECKING, Any

from pytest import fixture
from pytest_regressions.file_regression import FileRegressionFixture

from utilities.git import get_repo_root
from utilities.pytest import node_id_to_path

if TYPE_CHECKING:
    from polars import DataFrame
    from pytest import FixtureRequest

    from utilities.types import PathLike


_PATH_TESTS = Path("src", "tests")


##


class OrjsonRegressionFixture:
    """Implementation of `orjson_regression_fixture` fixture."""

    def __init__(self, path: PathLike, request: FixtureRequest, /) -> None:
        super().__init__()
        path = Path(path)
        datadir = path.parent
        self._fixture = FileRegressionFixture(
            datadir=datadir, original_datadir=datadir, request=request
        )
        self._basename = path.name

    def check(self, obj: Any, /, *, suffix: str | None = None) -> None:
        """Check the serialization of the object against the baseline."""
        from utilities.orjson import serialize

        data = serialize(obj)
        basename = self._basename
        if suffix is not None:
            basename = f"{basename}__{suffix}"
        self._fixture.check(
            data,
            extension=".json",
            basename=basename,
            binary=True,
            check_fn=self._check_fn,
        )

    def _check_fn(self, left: Path, right: Path, /) -> None:
        with left.open(mode="r") as fh:
            obj_x = loads(fh.read())
        with right.open(mode="r") as fh:
            obj_y = loads(fh.read())
        assert obj_x == obj_y


@fixture
def orjson_regression_fixture(*, request: FixtureRequest) -> OrjsonRegressionFixture:
    """Instance of the `OrjsonRegressionFixture`."""
    path = _get_path(request)
    return OrjsonRegressionFixture(path, request)


##


class PolarsDataFrameRegressionFixture:
    """Implementation of `polars_dataframe_regression_fixture`."""

    def __init__(self, path: PathLike, request: FixtureRequest, /) -> None:
        super().__init__()
        self._fixture = OrjsonRegressionFixture(path, request)

    def check(self, df: DataFrame, /, *, suffix: str | None = None) -> None:
        """Check the DataFrame summary against the baseline."""
        from polars import col
        from polars.exceptions import InvalidOperationError

        approx_n_unique: dict[str, int] = {}
        for column in df.columns:
            with suppress(InvalidOperationError):
                approx_n_unique[column] = df.select(
                    col(column).approx_n_unique()
                ).item()
        data = {
            "approx_n_unique": approx_n_unique,
            "describe": df.describe(percentiles=[i / 10 for i in range(1, 10)]).rows(
                named=True
            ),
            "estimated_size": df.estimated_size(),
            "glimpse": df.glimpse(return_as_string=True),
            "is_empty": df.is_empty(),
            "n_unique": df.n_unique(),
            "null_count": df.null_count().row(0, named=True),
        }
        self._fixture.check(data, suffix=suffix)


@fixture
def polars_dataframe_regression_fixture(
    *, request: FixtureRequest
) -> PolarsDataFrameRegressionFixture:
    """Instance of the `PolarsDataFrameRegressionFixture`."""
    path = _get_path(request)
    return PolarsDataFrameRegressionFixture(path, request)


##


def _get_path(request: FixtureRequest, /) -> Path:
    tail = node_id_to_path(request.node.nodeid, head=_PATH_TESTS)
    return get_repo_root().joinpath(_PATH_TESTS, "regressions", tail)


__all__ = [
    "OrjsonRegressionFixture",
    "PolarsDataFrameRegressionFixture",
    "orjson_regression_fixture",
    "polars_dataframe_regression_fixture",
]
