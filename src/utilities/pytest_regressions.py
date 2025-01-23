from __future__ import annotations

from contextlib import suppress
from json import loads
from pathlib import Path
from typing import TYPE_CHECKING, Any, assert_never

from pytest import fixture
from pytest_regressions.file_regression import FileRegressionFixture

from utilities.git import get_repo_root
from utilities.pytest import node_id_to_path

if TYPE_CHECKING:
    from polars import DataFrame, Series
    from pytest import FixtureRequest

    from utilities.types import PathLike, StrMapping


_PATH_TESTS = Path("src", "tests")


##


class OrjsonRegressionFixture:
    """Implementation of `orjson_regression` fixture."""

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
def orjson_regression(*, request: FixtureRequest) -> OrjsonRegressionFixture:
    """Instance of the `OrjsonRegressionFixture`."""
    path = _get_path(request)
    return OrjsonRegressionFixture(path, request)


##


class PolarsRegressionFixture:
    """Implementation of `polars_regression`."""

    def __init__(self, path: PathLike, request: FixtureRequest, /) -> None:
        super().__init__()
        self._fixture = OrjsonRegressionFixture(path, request)

    def check(self, obj: Series | DataFrame, /, *, suffix: str | None = None) -> None:
        """Check the Series/DataFrame summary against the baseline."""
        from polars import DataFrame, Series, col
        from polars.exceptions import InvalidOperationError

        data: StrMapping = {}
        match obj:
            case Series() as series:
                data["has_nulls"] = series.has_nulls()
                data["is_sorted"] = series.is_sorted()
                data["len"] = series.len()
                data["n_unique"] = series.n_unique()
                data["null_count"] = series.null_count()
            case DataFrame() as df:
                approx_n_unique: dict[str, int] = {}
                for column in df.columns:
                    with suppress(InvalidOperationError):
                        approx_n_unique[column] = df.select(
                            col(column).approx_n_unique()
                        ).item()
                data["approx_n_unique"] = approx_n_unique
                data["glimpse"] = df.glimpse(return_as_string=True)
                data["n_unique"] = df.n_unique()
                data["null_count"] = df.null_count().row(0, named=True)
            case _ as never:
                assert_never(never)
        data["describe"] = obj.describe(
            percentiles=[i / 10 for i in range(1, 10)]
        ).rows(named=True)
        data["estimated_size"] = obj.estimated_size()
        data["is_empty"] = obj.is_empty()
        self._fixture.check(data, suffix=suffix)


@fixture
def polars_regression(*, request: FixtureRequest) -> PolarsRegressionFixture:
    """Instance of the `PolarsRegressionFixture`."""
    path = _get_path(request)
    return PolarsRegressionFixture(path, request)


##


def _get_path(request: FixtureRequest, /) -> Path:
    tail = node_id_to_path(request.node.nodeid, head=_PATH_TESTS)
    return get_repo_root().joinpath(_PATH_TESTS, "regressions", tail)


__all__ = [
    "OrjsonRegressionFixture",
    "PolarsRegressionFixture",
    "orjson_regression",
    "polars_regression",
]
