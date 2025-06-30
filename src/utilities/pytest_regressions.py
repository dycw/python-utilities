from __future__ import annotations

from abc import ABC, abstractmethod
from contextlib import suppress
from dataclasses import dataclass
from functools import partial
from pathlib import Path
from typing import TYPE_CHECKING, Any, assert_never, cast, override

from polars.testing import assert_frame_equal
from pytest_regressions.common import perform_regression_check

from utilities.operator import is_equal
from utilities.pathlib import ensure_suffix

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping
    from collections.abc import Set as AbstractSet

    from polars import DataFrame, Series
    from pytest import FixtureRequest

    from utilities.orjson import _DataclassHook
    from utilities.types import StrMapping


@dataclass(order=True, unsafe_hash=True, kw_only=True)
class _BaseFixture(ABC):
    """Base class for all fixtures."""

    request: FixtureRequest
    tmp_path: Path

    @abstractmethod
    def check(self, obj: Any, /, *, suffix: str | None = None) -> None:
        """Check the object."""
        raise NotImplementedError(obj, suffix)

    def _check_fn(self, path1: Path, path2: Path, /) -> None:
        raise NotImplementedError(path1, path2)

    @abstractmethod
    def _dump_fn(self, path: Path, /, *, obj: Any) -> None:
        raise NotImplementedError(path, obj)

    def _get_path(self, suffix: str, /, *, opt_suffix: str | None = None) -> Path:
        path = _get_path(self.request)
        if opt_suffix is not None:
            path = Path(f"{path}__{opt_suffix}")
        return ensure_suffix(path, suffix)

    def _perform_regression_check(
        self,
        suffix: str,
        /,
        *,
        check_fn: Callable[[Path, Path], None],
        dump_fn: Callable[[Path], None],
        opt_suffix: str | None = None,
    ) -> None:
        perform_regression_check(
            datadir=NotImplemented,
            original_datadir=NotImplemented,
            request=self.request,
            check_fn=check_fn,
            dump_fn=dump_fn,
            extension=NotImplemented,
            fullpath=self._get_path(suffix, opt_suffix=opt_suffix),
            force_regen=False,
            obtained_filename=self._tmp_file,
        )

    @property
    def _tmp_file(self) -> Path:
        return self.tmp_path.joinpath("file")


##


@dataclass(order=True, unsafe_hash=True, kw_only=True)
class OrjsonRegressionFixture(_BaseFixture):
    """Regression fixture using `orjson` to serialize its contents."""

    @override
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
        self._dump_fn(
            self._tmp_file,
            obj=obj,
            before=before,
            globalns=globalns,
            localns=localns,
            warn_name_errors=warn_name_errors,
            dataclass_hook=dataclass_hook,
            dataclass_defaults=dataclass_defaults,
            compress=compress,
        )
        self._perform_regression_check(
            ".json.gz" if compress else ".json",
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
            opt_suffix=suffix,
        )

    @override
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
        from utilities.orjson import read_object

        left, right = [
            read_object(
                p,
                decompress=decompress,
                dataclass_hook=dataclass_hook,
                objects=objects,
                redirects=redirects,
            )
            for p in [path1, path2]
        ]
        assert is_equal(left, right), f"{left=}, {right=}"

    @override
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
        from utilities.orjson import write_object

        write_object(
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


@dataclass(order=True, unsafe_hash=True, kw_only=True)
class DataFrameRegressionFixture(_BaseFixture):
    """Regression fixture using `orjson` to serialize a DataFrame."""

    @override
    def check(
        self,
        df: DataFrame,
        /,
        *,
        summary: bool = False,
        compress: bool = False,
        check_row_order: bool = True,
        check_column_order: bool = True,
        check_dtypes: bool = True,
        check_exact: bool = False,
        rtol: float = 0.00001,
        atol: float = 1e-8,
        categorical_as_str: bool = False,
        suffix: str | None = None,
    ) -> None:
        """Check the Series/DataFrame summary against the baseline."""
        self._dump_fn(self._tmp_file, obj=df, summary=summary, compress=compress)
        self._perform_regression_check(
            ".json.gz" if compress else ".json",
            check_fn=partial(
                self._check_fn,
                decompress=compress,
                summary=summary,
                check_row_order=check_row_order,
                check_column_order=check_column_order,
                check_dtypes=check_dtypes,
                check_exact=check_exact,
                rtol=rtol,
                atol=atol,
                categorical_as_str=categorical_as_str,
            ),
            dump_fn=partial(self._dump_fn, obj=df, compress=compress),
            opt_suffix=suffix,
        )

    @override
    def _check_fn(
        self,
        path1: Path,
        path2: Path,
        /,
        *,
        decompress: bool = False,
        summary: bool = False,
        check_row_order: bool = True,
        check_column_order: bool = True,
        check_dtypes: bool = True,
        check_exact: bool = False,
        rtol: float = 0.00001,
        atol: float = 1e-8,
        categorical_as_str: bool = False,
    ) -> None:
        from utilities.orjson import read_object
        from utilities.polars import read_dataframe

        if summary:
            left, right = [
                read_object(p, decompress=decompress) for p in [path1, path2]
            ]
            assert is_equal(left, right), f"{left=}\n{right=}"
        else:
            left, right = [
                read_dataframe(p, decompress=decompress) for p in [path1, path2]
            ]
            assert_frame_equal(
                left,
                right,
                check_row_order=check_row_order,
                check_column_order=check_column_order,
                check_dtypes=check_dtypes,
                check_exact=check_exact,
                rtol=rtol,
                atol=atol,
                categorical_as_str=categorical_as_str,
            )

    @override
    def _dump_fn(
        self,
        path: Path,
        /,
        *,
        obj: DataFrame,
        summary: bool = False,
        compress: bool = False,
    ) -> None:
        from utilities.orjson import write_object
        from utilities.polars import write_dataframe

        if summary:
            write_object(
                _summarize_series_or_dataframe(obj),
                path,
                compress=compress,
                overwrite=True,
            )
        else:
            write_dataframe(obj, path, compress=compress, overwrite=True)


@dataclass(order=True, unsafe_hash=True, kw_only=True)
class SeriesRegressionFixture(_BaseFixture):
    """Regression fixture using `orjson` to serialize a Series."""

    @override
    def check(
        self,
        series: Series,
        /,
        *,
        summary: bool = False,
        compress: bool = False,
        suffix: str | None = None,
    ) -> None:
        """Check the Series against the baseline."""
        self._dump_fn(self._tmp_file, obj=series, summary=summary, compress=compress)
        self._perform_regression_check(
            ".json.gz" if compress else ".json",
            check_fn=partial(self._check_fn, decompress=compress, summary=summary),
            dump_fn=partial(self._dump_fn, obj=series, compress=compress),
            opt_suffix=suffix,
        )

    @override
    def _check_fn(
        self,
        path1: Path,
        path2: Path,
        /,
        *,
        decompress: bool = False,
        summary: bool = False,
        check_dtypes: bool = True,
        check_names: bool = True,
        check_order: bool = True,
        check_exact: bool = False,
        rtol: float = 1e-5,
        atol: float = 1e-8,
        categorical_as_str: bool = False,
    ) -> None:
        from polars.testing import assert_series_equal

        from utilities.orjson import read_object
        from utilities.polars import read_series

        if summary:
            left, right = [
                read_object(p, decompress=decompress) for p in [path1, path2]
            ]
            assert is_equal(left, right), f"{left=}\n{right=}"
        else:
            left, right = [
                read_series(p, decompress=decompress) for p in [path1, path2]
            ]
            assert_series_equal(
                left,
                right,
                check_dtypes=check_dtypes,
                check_names=check_names,
                check_order=check_order,
                check_exact=check_exact,
                rtol=rtol,
                atol=atol,
                categorical_as_str=categorical_as_str,
            )

    @override
    def _dump_fn(
        self,
        path: Path,
        /,
        *,
        obj: Series,
        summary: bool = False,
        compress: bool = False,
    ) -> None:
        from utilities.orjson import write_object
        from utilities.polars import write_series

        if summary:
            write_object(
                _summarize_series_or_dataframe(obj),
                path,
                compress=compress,
                overwrite=True,
            )
        else:
            write_series(obj, path, compress=compress, overwrite=True)


##


def _get_path(request: FixtureRequest, /) -> Path:
    from utilities.pathlib import get_root
    from utilities.pytest import node_id_path

    path = Path(cast("Any", request).fspath)
    root = Path("src", "tests")
    tail = node_id_path(request.node.nodeid, root=root)
    return get_root(path=path).joinpath(root, "regressions", tail)


def _summarize_series_or_dataframe(obj: Series | DataFrame, /) -> StrMapping:
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
    return data


__all__ = ["DataFrameRegressionFixture", "OrjsonRegressionFixture"]
