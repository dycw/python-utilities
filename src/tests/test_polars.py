from __future__ import annotations

import datetime as dt
from math import isfinite, nan
from typing import TYPE_CHECKING, Any, ClassVar, Literal

from polars import (
    DataFrame,
    Expr,
    Float64,
    Int64,
    Series,
    Utf8,
    col,
    concat,
    datetime_range,
    int_range,
    lit,
)
from polars.testing import assert_frame_equal, assert_series_equal
from pytest import mark, param, raises

from utilities.datetime import UTC
from utilities.polars import (
    CheckPolarsDataFrameError,
    ColumnsToDictError,
    EmptyPolarsConcatError,
    SetFirstRowAsColumnsError,
    _check_polars_dataframe_predicates,
    _check_polars_dataframe_schema_list,
    _check_polars_dataframe_schema_set,
    _check_polars_dataframe_schema_subset,
    ceil_datetime,
    check_polars_dataframe,
    collect_series,
    columns_to_dict,
    ensure_expr_or_series,
    floor_datetime,
    join,
    nan_sum_agg,
    nan_sum_cols,
    redirect_empty_polars_concat,
    set_first_row_as_columns,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

    from polars.type_aliases import IntoExprColumn, PolarsDataType, SchemaDict


class TestCeilDatetime:
    start: ClassVar[dt.datetime] = dt.datetime(2000, 1, 1, 0, 0, tzinfo=UTC)
    end: ClassVar[dt.datetime] = dt.datetime(2000, 1, 1, 0, 3, tzinfo=UTC)
    expected: ClassVar[Series] = Series(
        [
            dt.datetime(2000, 1, 1, 0, 0, tzinfo=UTC),
            dt.datetime(2000, 1, 1, 0, 1, tzinfo=UTC),
            dt.datetime(2000, 1, 1, 0, 1, tzinfo=UTC),
            dt.datetime(2000, 1, 1, 0, 1, tzinfo=UTC),
            dt.datetime(2000, 1, 1, 0, 1, tzinfo=UTC),
            dt.datetime(2000, 1, 1, 0, 1, tzinfo=UTC),
            dt.datetime(2000, 1, 1, 0, 1, tzinfo=UTC),
            dt.datetime(2000, 1, 1, 0, 2, tzinfo=UTC),
            dt.datetime(2000, 1, 1, 0, 2, tzinfo=UTC),
            dt.datetime(2000, 1, 1, 0, 2, tzinfo=UTC),
            dt.datetime(2000, 1, 1, 0, 2, tzinfo=UTC),
            dt.datetime(2000, 1, 1, 0, 2, tzinfo=UTC),
            dt.datetime(2000, 1, 1, 0, 2, tzinfo=UTC),
            dt.datetime(2000, 1, 1, 0, 3, tzinfo=UTC),
            dt.datetime(2000, 1, 1, 0, 3, tzinfo=UTC),
            dt.datetime(2000, 1, 1, 0, 3, tzinfo=UTC),
            dt.datetime(2000, 1, 1, 0, 3, tzinfo=UTC),
            dt.datetime(2000, 1, 1, 0, 3, tzinfo=UTC),
            dt.datetime(2000, 1, 1, 0, 3, tzinfo=UTC),
        ]
    )

    def test_expr(self) -> None:
        data = datetime_range(self.start, self.end, interval="10s")
        result = collect_series(ceil_datetime(data, "1m"))
        assert_series_equal(result, self.expected, check_names=False)

    def test_series(self) -> None:
        data = datetime_range(self.start, self.end, interval="10s", eager=True)
        result = ceil_datetime(data, "1m")
        assert_series_equal(result, self.expected, check_names=False)


class TestCheckPolarsDataFrame:
    def test_main(self) -> None:
        data = DataFrame()
        check_polars_dataframe(data)

    def test_columns_pass(self) -> None:
        data = DataFrame()
        check_polars_dataframe(data, columns=[])

    def test_columns_error(self) -> None:
        data = DataFrame()
        with raises(
            CheckPolarsDataFrameError,
            match="DataFrame must have columns .*; got .*\n\n.*",
        ):
            check_polars_dataframe(data, columns=["value"])

    def test_dtypes_pass(self) -> None:
        data = DataFrame()
        check_polars_dataframe(data, dtypes=[])

    def test_dtypes_error(self) -> None:
        data = DataFrame()
        with raises(
            CheckPolarsDataFrameError,
            match="DataFrame must have dtypes .*; got .*\n\n.*",
        ):
            check_polars_dataframe(data, dtypes=[Float64])

    def test_height_pass(self) -> None:
        data = DataFrame({"value": [0.0]})
        check_polars_dataframe(data, height=1)

    def test_height_error(self) -> None:
        data = DataFrame({"value": [0.0]})
        with raises(
            CheckPolarsDataFrameError,
            match="DataFrame must satisfy the height requirements; got .*\n\n.*",
        ):
            check_polars_dataframe(data, height=2)

    def test_min_height_pass(self) -> None:
        data = DataFrame({"value": [0.0, 1.0]})
        check_polars_dataframe(data, min_height=1)

    def test_min_height_error(self) -> None:
        data = DataFrame()
        with raises(
            CheckPolarsDataFrameError,
            match="DataFrame must satisfy the height requirements; got .*\n\n.*",
        ):
            check_polars_dataframe(data, min_height=1)

    def test_max_height_pass(self) -> None:
        data = DataFrame()
        check_polars_dataframe(data, max_height=1)

    def test_max_height_error(self) -> None:
        data = DataFrame({"value": [0.0, 1.0]})
        with raises(
            CheckPolarsDataFrameError,
            match="DataFrame must satisfy the height requirements; got .*\n\n.*",
        ):
            check_polars_dataframe(data, max_height=1)

    def test_predicates_pass(self) -> None:
        data = DataFrame({"value": [0.0, 1.0]})
        check_polars_dataframe(data, predicates={"value": isfinite})

    def test_predicates_error_missing_columns_and_failed(self) -> None:
        data = DataFrame({"a": [0.0, nan], "b": [0.0, nan]})
        with raises(
            CheckPolarsDataFrameError,
            match="DataFrame must satisfy the predicates; missing columns were .* and failed predicates were .*\n\n.*",
        ):
            check_polars_dataframe(data, predicates={"a": isfinite, "c": isfinite})

    def test_predicates_error_missing_columns_only(self) -> None:
        data = DataFrame()
        with raises(
            CheckPolarsDataFrameError,
            match="DataFrame must satisfy the predicates; missing columns were .*\n\n.*",
        ):
            check_polars_dataframe(data, predicates={"a": isfinite})

    def test_predicates_error_failed_only(self) -> None:
        data = DataFrame({"a": [0.0, nan]})
        with raises(
            CheckPolarsDataFrameError,
            match="DataFrame must satisfy the predicates; failed predicates were .*\n\n.*",
        ):
            check_polars_dataframe(data, predicates={"a": isfinite})

    def test_schema_list_pass(self) -> None:
        data = DataFrame()
        check_polars_dataframe(data, schema_list={})

    def test_schema_list_error_set_of_columns(self) -> None:
        data = DataFrame()
        with raises(
            CheckPolarsDataFrameError,
            match="DataFrame must have schema .*; got .*\n\n.*",
        ):
            check_polars_dataframe(data, schema_list={"value": Float64})

    def test_schema_list_error_order_of_columns(self) -> None:
        data = DataFrame(schema={"a": Float64, "b": Float64})
        with raises(
            CheckPolarsDataFrameError,
            match="DataFrame must have schema .*; got .*\n\n.*",
        ):
            check_polars_dataframe(data, schema_list={"b": Float64, "a": Float64})

    def test_schema_set_pass(self) -> None:
        data = DataFrame(schema={"a": Float64, "b": Float64})
        check_polars_dataframe(data, schema_set={"b": Float64, "a": Float64})

    def test_schema_set_error_set_of_columns(self) -> None:
        data = DataFrame()
        with raises(
            CheckPolarsDataFrameError,
            match="DataFrame must have schema .*; got .*\n\n.*",
        ):
            check_polars_dataframe(data, schema_set={"value": Float64})

    def test_schema_subset_pass(self) -> None:
        data = DataFrame({"foo": [0.0], "bar": [0.0]})
        check_polars_dataframe(data, schema_subset={"foo": Float64})

    def test_schema_subset_error(self) -> None:
        data = DataFrame({"foo": [0.0]})
        with raises(
            CheckPolarsDataFrameError,
            match="DataFrame schema must include .*; got .*\n\n.*",
        ):
            check_polars_dataframe(data, schema_subset={"bar": Float64})

    def test_shape_pass(self) -> None:
        data = DataFrame()
        check_polars_dataframe(data, shape=(0, 0))

    def test_shape_error(self) -> None:
        data = DataFrame()
        with raises(
            CheckPolarsDataFrameError,
            match="DataFrame must have shape .*; got .*\n\n.*",
        ):
            check_polars_dataframe(data, shape=(1, 1))

    def test_sorted_pass(self) -> None:
        data = DataFrame({"value": [0.0, 1.0]})
        check_polars_dataframe(data, sorted="value")

    def test_sorted_error(self) -> None:
        data = DataFrame({"value": [1.0, 0.0]})
        with raises(
            CheckPolarsDataFrameError, match="DataFrame must be sorted on .*\n\n.*"
        ):
            check_polars_dataframe(data, sorted="value")

    def test_unique_pass(self) -> None:
        data = DataFrame({"value": [0.0, 1.0]})
        check_polars_dataframe(data, unique="value")

    def test_unique_error(self) -> None:
        data = DataFrame({"value": [0.0, 0.0]})
        with raises(
            CheckPolarsDataFrameError, match="DataFrame must be unique on .*\n\n.*"
        ):
            check_polars_dataframe(data, unique="value")

    def test_width_pass(self) -> None:
        data = DataFrame()
        check_polars_dataframe(data, width=0)

    def test_width_error(self) -> None:
        data = DataFrame()
        with raises(
            CheckPolarsDataFrameError,
            match="DataFrame must have width .*; got .*\n\n.*",
        ):
            check_polars_dataframe(data, width=1)


class TestCheckPolarsDataFramePredicates:
    def test_pass(self) -> None:
        data = DataFrame({"value": [0.0, 1.0]})
        _check_polars_dataframe_predicates(data, {"value": isfinite})

    @mark.parametrize(
        "predicates",
        [
            param({"other": Float64}, id="missing column"),
            param({"value": isfinite}, id="failed"),
        ],
    )
    def test_error(self, *, predicates: Mapping[str, Callable[[Any], bool]]) -> None:
        data = DataFrame({"value": [0.0, nan]})
        with raises(CheckPolarsDataFrameError):
            _check_polars_dataframe_predicates(data, predicates)


class TestCheckPolarsDataFrameSchemaList:
    def test_pass(self) -> None:
        data = DataFrame({"value": [0.0]})
        _check_polars_dataframe_schema_list(data, {"value": Float64})

    def test_error(self) -> None:
        data = DataFrame()
        with raises(CheckPolarsDataFrameError):
            _check_polars_dataframe_schema_list(data, {"value": Float64})


class TestCheckPolarsDataFrameSchemaSet:
    def test_pass(self) -> None:
        data = DataFrame({"foo": [0.0], "bar": [0.0]})
        _check_polars_dataframe_schema_set(data, {"bar": Float64, "foo": Float64})

    def test_error(self) -> None:
        data = DataFrame()
        with raises(CheckPolarsDataFrameError):
            _check_polars_dataframe_schema_set(data, {"value": Float64})


class TestCheckPolarsDataFrameSchemaSubset:
    def test_pass(self) -> None:
        data = DataFrame({"foo": [0.0], "bar": [0.0]})
        _check_polars_dataframe_schema_subset(data, {"foo": Float64})

    @mark.parametrize(
        "schema_inc",
        [
            param({"bar": Float64}, id="missing column"),
            param({"foo": Int64}, id="wrong dtype"),
        ],
    )
    def test_error(self, *, schema_inc: SchemaDict) -> None:
        data = DataFrame({"foo": [0.0]})
        with raises(CheckPolarsDataFrameError):
            _check_polars_dataframe_schema_subset(data, schema_inc)


class TestCollectSeries:
    def test_main(self) -> None:
        expr = int_range(end=10)
        series = collect_series(expr)
        expected = int_range(end=10, eager=True)
        assert_series_equal(series, expected)


class TestColumnsToDict:
    def test_main(self) -> None:
        data = DataFrame(
            [{"a": 1, "b": 11}, {"a": 2, "b": 12}, {"a": 3, "b": 13}],
            schema={"a": Int64, "b": Int64},
        )
        mapping = columns_to_dict(data, "a", "b")
        assert mapping == {1: 11, 2: 12, 3: 13}

    def test_error(self) -> None:
        data = DataFrame(
            [{"a": 1, "b": 11}, {"a": 2, "b": 12}, {"a": 1, "b": 13}],
            schema={"a": Int64, "b": Int64},
        )
        with raises(ColumnsToDictError, match="DataFrame must be unique on 'a'"):
            _ = columns_to_dict(data, "a", "b")


class TestEnsureExprOrSeries:
    @mark.parametrize(
        "column", [param("column"), param(col("column")), param(int_range(end=10))]
    )
    def test_main(self, *, column: IntoExprColumn) -> None:
        result = ensure_expr_or_series(column)
        assert isinstance(result, Expr | Series)


class TestFloorDatetime:
    start: ClassVar[dt.datetime] = dt.datetime(2000, 1, 1, 0, 0, tzinfo=UTC)
    end: ClassVar[dt.datetime] = dt.datetime(2000, 1, 1, 0, 3, tzinfo=UTC)
    expected: ClassVar[Series] = Series(
        [
            dt.datetime(2000, 1, 1, 0, 0, tzinfo=UTC),
            dt.datetime(2000, 1, 1, 0, 0, tzinfo=UTC),
            dt.datetime(2000, 1, 1, 0, 0, tzinfo=UTC),
            dt.datetime(2000, 1, 1, 0, 0, tzinfo=UTC),
            dt.datetime(2000, 1, 1, 0, 0, tzinfo=UTC),
            dt.datetime(2000, 1, 1, 0, 0, tzinfo=UTC),
            dt.datetime(2000, 1, 1, 0, 1, tzinfo=UTC),
            dt.datetime(2000, 1, 1, 0, 1, tzinfo=UTC),
            dt.datetime(2000, 1, 1, 0, 1, tzinfo=UTC),
            dt.datetime(2000, 1, 1, 0, 1, tzinfo=UTC),
            dt.datetime(2000, 1, 1, 0, 1, tzinfo=UTC),
            dt.datetime(2000, 1, 1, 0, 1, tzinfo=UTC),
            dt.datetime(2000, 1, 1, 0, 2, tzinfo=UTC),
            dt.datetime(2000, 1, 1, 0, 2, tzinfo=UTC),
            dt.datetime(2000, 1, 1, 0, 2, tzinfo=UTC),
            dt.datetime(2000, 1, 1, 0, 2, tzinfo=UTC),
            dt.datetime(2000, 1, 1, 0, 2, tzinfo=UTC),
            dt.datetime(2000, 1, 1, 0, 2, tzinfo=UTC),
            dt.datetime(2000, 1, 1, 0, 3, tzinfo=UTC),
        ]
    )

    def test_expr(self) -> None:
        data = datetime_range(self.start, self.end, interval="10s")
        result = collect_series(floor_datetime(data, "1m"))
        assert_series_equal(result, self.expected, check_names=False)

    def test_series(self) -> None:
        data = datetime_range(self.start, self.end, interval="10s", eager=True)
        result = floor_datetime(data, "1m")
        assert_series_equal(result, self.expected, check_names=False)


class TestJoin:
    def test_main(self) -> None:
        df1 = DataFrame([{"a": 1, "b": 2}], schema={"a": Int64, "b": Int64})
        df2 = DataFrame([{"a": 1, "c": 3}], schema={"a": Int64, "c": Int64})
        result = join(df1, df2, on="a")
        expected = DataFrame(
            [{"a": 1, "b": 2, "c": 3}], schema={"a": Int64, "b": Int64, "c": Int64}
        )
        assert_frame_equal(result, expected)


class TestNanSumAgg:
    @mark.parametrize(
        ("data_values", "expected"),
        [
            param([None], None, id="one None"),
            param([None, None], None, id="two Nones"),
            param([0], 0, id="one int"),
            param([0, None], 0, id="one int, one None"),
            param([0, None, None], 0, id="one int, two Nones"),
            param([1, 2], 3, id="two ints"),
            param([1, 2, None], 3, id="two ints, one None"),
            param([1, 2, None, None], 3, id="two ints, two Nones"),
        ],
    )
    @mark.parametrize("dtype", [param(Int64), param(Float64)])
    @mark.parametrize("mode", [param("str"), param("column")])
    def test_main(
        self,
        *,
        data_values: list[Any],
        expected: int | None,
        dtype: PolarsDataType,
        mode: Literal["str", "column"],
    ) -> None:
        data = DataFrame(data=data_values, schema={"value": dtype}).with_columns(
            id=lit("id")
        )
        match mode:
            case "str":
                agg = "value"
            case "column":
                agg = col("value")
        result = data.group_by("id").agg(nan_sum_agg(agg))
        assert result["value"].item() == expected


class TestNanSumCols:
    @mark.parametrize(
        ("x", "y", "expected"),
        [param(None, None, None), param(None, 0, 0), param(0, None, 0), param(1, 2, 3)],
    )
    @mark.parametrize("x_kind", [param("str"), param("column")])
    @mark.parametrize("y_kind", [param("str"), param("column")])
    def test_main(
        self,
        *,
        x: int | None,
        y: int | None,
        expected: int | None,
        x_kind: Literal["str", "column"],
        y_kind: Literal["str", "column"],
    ) -> None:
        x_use = "x" if x_kind == "str" else col("x")
        y_use = "y" if y_kind == "str" else col("y")
        data = DataFrame([(x, y)], schema={"x": Int64, "y": Int64}).with_columns(
            z=nan_sum_cols(x_use, y_use)
        )
        assert data["z"].item() == expected


class TestRedirectEmptyPolarsConcat:
    def test_main(self) -> None:
        with raises(EmptyPolarsConcatError), redirect_empty_polars_concat():
            _ = concat([])


class TestSetFirstRowAsColumns:
    def test_empty(self) -> None:
        data = DataFrame()
        with raises(SetFirstRowAsColumnsError):
            _ = set_first_row_as_columns(data)

    def test_one_row(self) -> None:
        data = DataFrame(["value"])
        check_polars_dataframe(data, height=1, schema_list={"column_0": Utf8})
        result = set_first_row_as_columns(data)
        check_polars_dataframe(result, height=0, schema_list={"value": Utf8})

    def test_multiple_rows(self) -> None:
        data = DataFrame(["foo", "bar", "baz"])
        check_polars_dataframe(data, height=3, schema_list={"column_0": Utf8})
        result = set_first_row_as_columns(data)
        check_polars_dataframe(result, height=2, schema_list={"foo": Utf8})
