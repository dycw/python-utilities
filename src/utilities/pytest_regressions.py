from __future__ import annotations

from contextlib import suppress
from dataclasses import dataclass
from functools import partial
from pathlib import Path
from typing import TYPE_CHECKING, Any, assert_never, cast

from pytest_regressions.common import perform_regression_check

from utilities.operator import is_equal
from utilities.pathlib import ensure_suffix

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping
    from collections.abc import Set as AbstractSet

    from polars import DataFrame, Series
    from pytest import FixtureRequest

    from utilities.orjson import _DataclassHook
    from utilities.types import PathLike, StrMapping


##


@dataclass(order=True, unsafe_hash=True, kw_only=True)
class OrjsonRegressionFixture:
    """Implementation of `orjson_regression` fixture."""

    request: FixtureRequest
    tmp_path: Path

    # def __post_init__(self) -> None:
    #     original_datadir = path.parent
    #     data_dir = self.path = tmp_path.joinpath(ensure_str(request.fixturename))
    #     with suppress(FileNotFoundError):
    #         _ = copytree(original_datadir, data_dir)
    #     self._fixture = FileRegressionFixture(
    #         datadir=data_dir, original_datadir=original_datadir, request=request
    #     )
    #     self._basename = path.name

    def check(
        self,
        obj: Any,
        /,
        *,
        before: Callable[[Any], Any] | None = None,
        globalns: StrMapping | None = None,
        localns: StrMapping | None = None,
        warn_name_errors: bool = False,
        dataclass_hook: _DataclassHook | None = None,
        dataclass_defaults: bool = False,
        compress: bool = False,
        objects: AbstractSet[type[Any]] | None = None,
        redirects: Mapping[str, type[Any]] | None = None,
        suffix: str | None = None,
    ) -> None:
        """Check the serialization of the object against the baseline."""
        tmp = self.tmp_path.joinpath("file")
        self._dump_fn(
            tmp,
            obj=obj,
            before=before,
            globalns=globalns,
            localns=localns,
            warn_name_errors=warn_name_errors,
            dataclass_hook=dataclass_hook,
            dataclass_defaults=dataclass_defaults,
            compress=compress,
        )
        path = _get_path(self.request)
        if suffix is not None:
            path = Path(f"{path}__{suffix}")
        path = ensure_suffix(path, ".json.gz" if compress else ".json")
        perform_regression_check(
            datadir=NotImplemented,
            original_datadir=NotImplemented,
            request=self.request,
            check_fn=partial(
                self._check_fn,
                decompress=compress,
                dataclass_hook=dataclass_hook,
                objects=objects,
                redirects=redirects,
            ),
            dump_fn=partial(
                self._dump_fn,
                obj=obj,
                before=before,
                globalns=globalns,
                localns=localns,
                warn_name_errors=warn_name_errors,
                dataclass_hook=dataclass_hook,
                dataclass_defaults=dataclass_defaults,
                compress=compress,
            ),
            extension=NotImplemented,
            fullpath=path,
            force_regen=False,
            obtained_filename=tmp,
        )

    def _check_fn(
        self,
        path1: Path,
        path2: Path,
        /,
        *,
        decompress: bool = False,
        dataclass_hook: _DataclassHook | None = None,
        objects: AbstractSet[type[Any]] | None = None,
        redirects: Mapping[str, type[Any]] | None = None,
    ) -> None:
        from utilities.orjson import read_json

        left, right = [
            read_json(
                p,
                decompress=decompress,
                dataclass_hook=dataclass_hook,
                objects=objects,
                redirects=redirects,
            )
            for p in [path1, path2]
        ]
        assert is_equal(left, right), f"{left=}, {right=}"

    def _dump_fn(
        self,
        path: Path,
        /,
        *,
        obj: Any,
        before: Callable[[Any], Any] | None = None,
        globalns: StrMapping | None = None,
        localns: StrMapping | None = None,
        warn_name_errors: bool = False,
        dataclass_hook: _DataclassHook | None = None,
        dataclass_defaults: bool = False,
        compress: bool = False,
    ) -> None:
        from utilities.orjson import write_json

        write_json(
            obj,
            path,
            before=before,
            globalns=globalns,
            localns=localns,
            warn_name_errors=warn_name_errors,
            dataclass_hook=dataclass_hook,
            dataclass_defaults=dataclass_defaults,
            compress=compress,
            overwrite=True,
        )


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
            "estimated_size": obj.estimated_size(),
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
                data["glimpse"] = df.glimpse(return_as_string=True)
                data["null_count"] = df.null_count().row(0, named=True)
            case _ as never:
                assert_never(never)
        self._fixture.check(data, suffix=suffix)


##


def _get_path(request: FixtureRequest, /) -> Path:
    from utilities.pathlib import get_root
    from utilities.pytest import node_id_path

    path = Path(cast("Any", request).fspath)
    root = Path("src", "tests")
    tail = node_id_path(request.node.nodeid, root=root)
    return get_root(path=path).joinpath(root, "regressions", tail)


__all__ = ["OrjsonRegressionFixture", "PolarsRegressionFixture"]
