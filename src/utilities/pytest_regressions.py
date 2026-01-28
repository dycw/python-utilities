from __future__ import annotations

from contextlib import suppress
from dataclasses import dataclass
from json import loads
from pathlib import Path
from typing import TYPE_CHECKING, Any, assert_never, override

from pytest_datadir.plugin import LazyDataDir
from pytest_regressions.file_regression import FileRegressionFixture
from rich.pretty import pretty_repr

from utilities._core_errors import CopySourceNotFoundError
from utilities.core import copy
from utilities.functions import ensure_str
from utilities.operator import is_equal

if TYPE_CHECKING:
    from polars import DataFrame, Series
    from pytest import FixtureRequest

    from utilities.types import PathLike, StrMapping


##


class OrjsonRegressionFixture:
    """Implementation of `orjson_regression` fixture."""

    def __init__(
        self, path: PathLike, request: FixtureRequest, tmp_path: Path, /
    ) -> None:
        super().__init__()
        path = Path(path)
        original_datadir = path.parent
        data_dir = tmp_path.joinpath(ensure_str(request.fixturename))
        with suppress(CopySourceNotFoundError):
            copy(original_datadir, data_dir, overwrite=True)
        self._fixture = FileRegressionFixture(
            datadir=LazyDataDir(original_datadir=original_datadir, tmp_path=data_dir),
            original_datadir=original_datadir,
            request=request,
        )
        self._basename = path.name

    def check(
        self,
        obj: Any,
        /,
        *,
        globalns: StrMapping | None = None,
        localns: StrMapping | None = None,
        warn_name_errors: bool = False,
        dataclass_defaults: bool = False,
        suffix: str | None = None,
    ) -> None:
        """Check the serialization of the object against the baseline."""
        from utilities.orjson import serialize

        data = serialize(
            obj,
            globalns=globalns,
            localns=localns,
            warn_name_errors=warn_name_errors,
            dataclass_defaults=dataclass_defaults,
        )
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

    def _check_fn(self, path_obtained: Path, path_existing: Path, /) -> None:
        obtained = loads(path_obtained.read_text())
        existing = loads(path_existing.read_text())
        if not is_equal(obtained, existing):
            raise OrjsonRegressionError(
                path_obtained=path_obtained,
                path_existing=path_existing,
                obtained=obtained,
                existing=existing,
            )


@dataclass(kw_only=True, slots=True)
class OrjsonRegressionError(Exception):
    path_obtained: Path
    path_existing: Path
    obtained: Any
    existing: Any

    @override
    def __str__(self) -> str:
        return f"Obtained object (at {pretty_repr(str(self.path_obtained))}) and existing object (at {pretty_repr(str(self.path_existing))}) differ; got {pretty_repr(self.obtained)} and {pretty_repr(self.existing)}"


##


class PolarsRegressionFixture:
    """Implementation of `polars_regression`."""

    def __init__(
        self, path: PathLike, request: FixtureRequest, tmp_path: Path, /
    ) -> None:
        super().__init__()
        self._fixture = OrjsonRegressionFixture(path, request, tmp_path)

    def check(self, obj: Series | DataFrame, /, *, suffix: str | None = None) -> None:
        """Check the Series/DataFrame summary against the baseline."""
        from polars import DataFrame, Series, col
        from polars.exceptions import InvalidOperationError

        data: StrMapping = {
            "describe": obj.describe(percentiles=[i / 10 for i in range(1, 10)]).rows(
                named=True
            ),
            "is_empty": obj.is_empty(),
            "n_unique": obj.n_unique(),
        }
        match obj:
            case Series() as series:
                data["has_nulls"] = series.has_nulls()
                data["is_sorted"] = series.is_sorted()
                data["len"] = series.len()
                data["null_count"] = series.null_count()
            case DataFrame() as df:
                approx_n_unique: dict[str, int] = {}
                for column in df.columns:
                    with suppress(InvalidOperationError):
                        approx_n_unique[column] = df.select(
                            col(column).approx_n_unique()
                        ).item()
                data["approx_n_unique"] = approx_n_unique
                data["glimpse"] = df.glimpse(return_type="string")
                data["null_count"] = df.null_count().row(0, named=True)
            case never:
                assert_never(never)
        self._fixture.check(data, suffix=suffix)


__all__ = ["OrjsonRegressionFixture", "PolarsRegressionFixture"]
