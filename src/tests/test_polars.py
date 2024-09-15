import datetime as dt
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from enum import Enum, auto
from math import isfinite, nan
from typing import Any, ClassVar, Literal, cast
from zoneinfo import ZoneInfo

from hypothesis import given
from hypothesis.strategies import sampled_from
from polars import (
    Boolean,
    DataFrame,
    Date,
    Datetime,
    Expr,
    Float64,
    Int64,
    List,
    Series,
    Struct,
    Utf8,
    col,
    datetime_range,
    int_range,
    lit,
)
from polars._typing import IntoExprColumn, PolarsDataType, SchemaDict
from polars.testing import assert_frame_equal, assert_series_equal
from pytest import mark, param, raises

from utilities.polars import (
    CheckPolarsDataFrameError,
    CheckZonedDTypeOrSeriesError,
    ColumnsToDictError,
    DatetimeHongKong,
    DatetimeTokyo,
    DatetimeUSCentral,
    DatetimeUSEastern,
    DatetimeUTC,
    DropNullStructSeriesError,
    IsNotNullStructSeriesError,
    IsNullStructSeriesError,
    SetFirstRowAsColumnsError,
    StructDataTypeError,
    YieldStructSeriesElementsError,
    _check_polars_dataframe_predicates,
    _check_polars_dataframe_schema_list,
    _check_polars_dataframe_schema_set,
    _check_polars_dataframe_schema_subset,
    _yield_struct_series_element_remove_nulls,
    ceil_datetime,
    check_polars_dataframe,
    check_zoned_dtype_or_series,
    collect_series,
    columns_to_dict,
    convert_time_zone,
    drop_null_struct_series,
    ensure_expr_or_series,
    floor_datetime,
    is_not_null_struct_series,
    is_null_struct_series,
    join,
    nan_sum_agg,
    nan_sum_cols,
    replace_time_zone,
    set_first_row_as_columns,
    struct_data_type,
    yield_struct_series_dataclasses,
    yield_struct_series_elements,
    zoned_datetime,
)
from utilities.zoneinfo import (
    UTC,
    HongKong,
    Tokyo,
    USCentral,
    USEastern,
    get_time_zone_name,
)


class TestCeilDatetime:
    start: ClassVar[dt.datetime] = dt.datetime(2000, 1, 1, 0, 0, tzinfo=UTC)
    end: ClassVar[dt.datetime] = dt.datetime(2000, 1, 1, 0, 3, tzinfo=UTC)
    expected: ClassVar[Series] = Series([
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
    ])

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
        df = DataFrame()
        check_polars_dataframe(df)

    def test_columns_pass(self) -> None:
        df = DataFrame()
        check_polars_dataframe(df, columns=[])

    def test_columns_error(self) -> None:
        df = DataFrame()
        with raises(
            CheckPolarsDataFrameError,
            match="DataFrame must have columns .*; got .*\n\n.*",
        ):
            check_polars_dataframe(df, columns=["value"])

    def test_dtypes_pass(self) -> None:
        df = DataFrame()
        check_polars_dataframe(df, dtypes=[])

    def test_dtypes_error(self) -> None:
        df = DataFrame()
        with raises(
            CheckPolarsDataFrameError,
            match="DataFrame must have dtypes .*; got .*\n\n.*",
        ):
            check_polars_dataframe(df, dtypes=[Float64])

    def test_height_pass(self) -> None:
        df = DataFrame({"value": [0.0]})
        check_polars_dataframe(df, height=1)

    def test_height_error(self) -> None:
        df = DataFrame({"value": [0.0]})
        with raises(
            CheckPolarsDataFrameError,
            match="DataFrame must satisfy the height requirements; got .*\n\n.*",
        ):
            check_polars_dataframe(df, height=2)

    def test_min_height_pass(self) -> None:
        df = DataFrame({"value": [0.0, 1.0]})
        check_polars_dataframe(df, min_height=1)

    def test_min_height_error(self) -> None:
        df = DataFrame()
        with raises(
            CheckPolarsDataFrameError,
            match="DataFrame must satisfy the height requirements; got .*\n\n.*",
        ):
            check_polars_dataframe(df, min_height=1)

    def test_max_height_pass(self) -> None:
        df = DataFrame()
        check_polars_dataframe(df, max_height=1)

    def test_max_height_error(self) -> None:
        df = DataFrame({"value": [0.0, 1.0]})
        with raises(
            CheckPolarsDataFrameError,
            match="DataFrame must satisfy the height requirements; got .*\n\n.*",
        ):
            check_polars_dataframe(df, max_height=1)

    def test_predicates_pass(self) -> None:
        df = DataFrame({"value": [0.0, 1.0]})
        check_polars_dataframe(df, predicates={"value": isfinite})

    def test_predicates_error_missing_columns_and_failed(self) -> None:
        df = DataFrame({"a": [0.0, nan], "b": [0.0, nan]})
        with raises(
            CheckPolarsDataFrameError,
            match="DataFrame must satisfy the predicates; missing columns were .* and failed predicates were .*\n\n.*",
        ):
            check_polars_dataframe(df, predicates={"a": isfinite, "c": isfinite})

    def test_predicates_error_missing_columns_only(self) -> None:
        df = DataFrame()
        with raises(
            CheckPolarsDataFrameError,
            match="DataFrame must satisfy the predicates; missing columns were .*\n\n.*",
        ):
            check_polars_dataframe(df, predicates={"a": isfinite})

    def test_predicates_error_failed_only(self) -> None:
        df = DataFrame({"a": [0.0, nan]})
        with raises(
            CheckPolarsDataFrameError,
            match="DataFrame must satisfy the predicates; failed predicates were .*\n\n.*",
        ):
            check_polars_dataframe(df, predicates={"a": isfinite})

    def test_schema_list_pass(self) -> None:
        df = DataFrame()
        check_polars_dataframe(df, schema_list={})

    def test_schema_list_error_set_of_columns(self) -> None:
        df = DataFrame()
        with raises(
            CheckPolarsDataFrameError,
            match="DataFrame must have schema .*; got .*\n\n.*",
        ):
            check_polars_dataframe(df, schema_list={"value": Float64})

    def test_schema_list_error_order_of_columns(self) -> None:
        df = DataFrame(schema={"a": Float64, "b": Float64})
        with raises(
            CheckPolarsDataFrameError,
            match="DataFrame must have schema .*; got .*\n\n.*",
        ):
            check_polars_dataframe(df, schema_list={"b": Float64, "a": Float64})

    def test_schema_set_pass(self) -> None:
        df = DataFrame(schema={"a": Float64, "b": Float64})
        check_polars_dataframe(df, schema_set={"b": Float64, "a": Float64})

    def test_schema_set_error_set_of_columns(self) -> None:
        df = DataFrame()
        with raises(
            CheckPolarsDataFrameError,
            match="DataFrame must have schema .*; got .*\n\n.*",
        ):
            check_polars_dataframe(df, schema_set={"value": Float64})

    def test_schema_subset_pass(self) -> None:
        df = DataFrame({"foo": [0.0], "bar": [0.0]})
        check_polars_dataframe(df, schema_subset={"foo": Float64})

    def test_schema_subset_error(self) -> None:
        df = DataFrame({"foo": [0.0]})
        with raises(
            CheckPolarsDataFrameError,
            match="DataFrame schema must include .*; got .*\n\n.*",
        ):
            check_polars_dataframe(df, schema_subset={"bar": Float64})

    def test_shape_pass(self) -> None:
        df = DataFrame()
        check_polars_dataframe(df, shape=(0, 0))

    def test_shape_error(self) -> None:
        df = DataFrame()
        with raises(
            CheckPolarsDataFrameError,
            match="DataFrame must have shape .*; got .*\n\n.*",
        ):
            check_polars_dataframe(df, shape=(1, 1))

    def test_sorted_pass(self) -> None:
        df = DataFrame({"value": [0.0, 1.0]})
        check_polars_dataframe(df, sorted="value")

    def test_sorted_error(self) -> None:
        df = DataFrame({"value": [1.0, 0.0]})
        with raises(
            CheckPolarsDataFrameError, match="DataFrame must be sorted on .*\n\n.*"
        ):
            check_polars_dataframe(df, sorted="value")

    def test_unique_pass(self) -> None:
        df = DataFrame({"value": [0.0, 1.0]})
        check_polars_dataframe(df, unique="value")

    def test_unique_error(self) -> None:
        df = DataFrame({"value": [0.0, 0.0]})
        with raises(
            CheckPolarsDataFrameError, match="DataFrame must be unique on .*\n\n.*"
        ):
            check_polars_dataframe(df, unique="value")

    def test_width_pass(self) -> None:
        df = DataFrame()
        check_polars_dataframe(df, width=0)

    def test_width_error(self) -> None:
        df = DataFrame()
        with raises(
            CheckPolarsDataFrameError,
            match="DataFrame must have width .*; got .*\n\n.*",
        ):
            check_polars_dataframe(df, width=1)


class TestCheckPolarsDataFramePredicates:
    def test_pass(self) -> None:
        df = DataFrame({"value": [0.0, 1.0]})
        _check_polars_dataframe_predicates(df, {"value": isfinite})

    @mark.parametrize(
        "predicates",
        [
            param({"other": Float64}, id="missing column"),
            param({"value": isfinite}, id="failed"),
        ],
    )
    def test_error(self, *, predicates: Mapping[str, Callable[[Any], bool]]) -> None:
        df = DataFrame({"value": [0.0, nan]})
        with raises(CheckPolarsDataFrameError):
            _check_polars_dataframe_predicates(df, predicates)


class TestCheckPolarsDataFrameSchemaList:
    def test_pass(self) -> None:
        df = DataFrame({"value": [0.0]})
        _check_polars_dataframe_schema_list(df, {"value": Float64})

    def test_error(self) -> None:
        df = DataFrame()
        with raises(CheckPolarsDataFrameError):
            _check_polars_dataframe_schema_list(df, {"value": Float64})


class TestCheckPolarsDataFrameSchemaSet:
    def test_pass(self) -> None:
        df = DataFrame({"foo": [0.0], "bar": [0.0]})
        _check_polars_dataframe_schema_set(df, {"bar": Float64, "foo": Float64})

    def test_error(self) -> None:
        df = DataFrame()
        with raises(CheckPolarsDataFrameError):
            _check_polars_dataframe_schema_set(df, {"value": Float64})


class TestCheckPolarsDataFrameSchemaSubset:
    def test_pass(self) -> None:
        df = DataFrame({"foo": [0.0], "bar": [0.0]})
        _check_polars_dataframe_schema_subset(df, {"foo": Float64})

    @mark.parametrize(
        "schema_inc",
        [
            param({"bar": Float64}, id="missing column"),
            param({"foo": Int64}, id="wrong dtype"),
        ],
    )
    def test_error(self, *, schema_inc: SchemaDict) -> None:
        df = DataFrame({"foo": [0.0]})
        with raises(CheckPolarsDataFrameError):
            _check_polars_dataframe_schema_subset(df, schema_inc)


class TestCheckZonedDTypeOrSeries:
    @given(time_zone=sampled_from([HongKong, UTC]))
    @mark.parametrize("case", [param("dtype"), param("series")])
    def test_main(
        self, *, time_zone: ZoneInfo, case: Literal["dtype", "series"]
    ) -> None:
        dtype = zoned_datetime(time_zone=time_zone)
        match case:
            case "dtype":
                dtype_or_series = dtype
            case "series":
                dtype_or_series = Series(dtype=dtype)
        check_zoned_dtype_or_series(dtype_or_series)

    def test_error_not_datetime(self) -> None:
        with raises(
            CheckZonedDTypeOrSeriesError,
            match="Data type must be Datetime; got Boolean",
        ):
            check_zoned_dtype_or_series(Boolean())

    def test_error_not_zoned(self) -> None:
        with raises(
            CheckZonedDTypeOrSeriesError, match="Data type must be zoned; got .*"
        ):
            check_zoned_dtype_or_series(Datetime())


class TestCollectSeries:
    def test_main(self) -> None:
        expr = int_range(end=10)
        series = collect_series(expr)
        expected = int_range(end=10, eager=True)
        assert_series_equal(series, expected)


class TestColumnsToDict:
    def test_main(self) -> None:
        df = DataFrame(
            [{"a": 1, "b": 11}, {"a": 2, "b": 12}, {"a": 3, "b": 13}],
            schema={"a": Int64, "b": Int64},
        )
        mapping = columns_to_dict(df, "a", "b")
        assert mapping == {1: 11, 2: 12, 3: 13}

    def test_error(self) -> None:
        df = DataFrame(
            [{"a": 1, "b": 11}, {"a": 2, "b": 12}, {"a": 1, "b": 13}],
            schema={"a": Int64, "b": Int64},
        )
        with raises(ColumnsToDictError, match="DataFrame must be unique on 'a'"):
            _ = columns_to_dict(df, "a", "b")


class TestConvertTimeZone:
    now_utc: ClassVar[dt.datetime] = dt.datetime(2000, 1, 1, 12, tzinfo=UTC)
    now_hkg: ClassVar[dt.datetime] = dt.datetime(2000, 1, 1, 20, tzinfo=HongKong)

    def test_series_datetime(self) -> None:
        series = Series(name="series", values=[self.now_utc], dtype=DatetimeUTC)
        result = convert_time_zone(series, time_zone=HongKong)
        expected = Series(name="series", values=[self.now_hkg], dtype=DatetimeHongKong)
        assert_series_equal(result, expected)

    def test_series_non_datetime(self) -> None:
        series = Series(name="series", values=[True], dtype=Boolean)
        result = convert_time_zone(series, time_zone=HongKong)
        assert_series_equal(result, series)

    def test_series_nested(self) -> None:
        series = Series(
            name="series",
            values=[{"datetime": self.now_utc, "boolean": True}],
            dtype=Struct({"datetime": DatetimeUTC, "boolean": Boolean}),
        )
        result = convert_time_zone(series, time_zone=HongKong)
        expected = Series(
            name="series",
            values=[{"datetime": self.now_hkg, "boolean": True}],
            dtype=Struct({"datetime": DatetimeHongKong, "boolean": Boolean}),
        )
        assert_series_equal(result, expected)

    def test_series_nested_twice(self) -> None:
        series = Series(
            name="series",
            values=[{"datetime": {"inner": self.now_utc}, "boolean": True}],
            dtype=Struct({
                "datetime": Struct({"inner": DatetimeUTC}),
                "boolean": Boolean,
            }),
        )
        result = convert_time_zone(series, time_zone=HongKong)
        expected = Series(
            name="series",
            values=[{"datetime": {"inner": self.now_hkg}, "boolean": True}],
            dtype=Struct({
                "datetime": Struct({"inner": DatetimeHongKong}),
                "boolean": Boolean,
            }),
        )
        assert_series_equal(result, expected)

    def test_dataframe_datetime(self) -> None:
        df = DataFrame(data=[self.now_utc], schema={"datetime": DatetimeUTC})
        result = convert_time_zone(df, time_zone=HongKong)
        expected = DataFrame(data=[self.now_hkg], schema={"datetime": DatetimeHongKong})
        assert_frame_equal(result, expected)

    def test_dataframe_non_datetime(self) -> None:
        df = DataFrame(data=[True], schema={"boolean": Boolean})
        result = convert_time_zone(df, time_zone=HongKong)
        expected = DataFrame(data=[True], schema={"boolean": Boolean})
        assert_frame_equal(result, expected)

    def test_dataframe_nested(self) -> None:
        df = DataFrame(
            data=[(self.now_utc, True)],
            schema={"datetime": DatetimeUTC, "boolean": Boolean},
            orient="row",
        )
        result = convert_time_zone(df, time_zone=HongKong)
        expected = DataFrame(
            data=[(self.now_hkg, True)],
            schema={"datetime": DatetimeHongKong, "boolean": Boolean},
            orient="row",
        )
        assert_frame_equal(result, expected)

    def test_dataframe_nested_twice(self) -> None:
        df = DataFrame(
            data=[((self.now_utc,), True)],
            schema={"datetime": Struct({"inner": DatetimeUTC}), "boolean": Boolean},
            orient="row",
        )
        result = convert_time_zone(df, time_zone=HongKong)
        expected = DataFrame(
            data=[((self.now_hkg,), True)],
            schema={
                "datetime": Struct({"inner": DatetimeHongKong}),
                "boolean": Boolean,
            },
            orient="row",
        )
        assert_frame_equal(result, expected)


class TestDatetimeUTC:
    @mark.parametrize(
        ("dtype", "time_zone"),
        [
            param(DatetimeHongKong, HongKong),
            param(DatetimeTokyo, Tokyo),
            param(DatetimeUSCentral, USCentral),
            param(DatetimeUSEastern, USEastern),
            param(DatetimeUTC, UTC),
        ],
    )
    def test_main(self, *, dtype: Datetime, time_zone: ZoneInfo) -> None:
        assert dtype.time_zone == get_time_zone_name(time_zone)


class TestDropNullStructSeries:
    def test_main(self) -> None:
        series = Series(
            values=[
                {"a": None, "b": None},
                {"a": True, "b": None},
                {"a": None, "b": False},
                {"a": True, "b": False},
            ],
            dtype=Struct({"a": Boolean, "b": Boolean}),
        )
        result = drop_null_struct_series(series)
        expected = series[1:]
        assert_series_equal(result, expected)

    def test_error(self) -> None:
        series = Series(name="series", values=[1, 2, 3, None], dtype=Int64)
        with raises(
            DropNullStructSeriesError, match="Series must have Struct-dtype; got Int64"
        ):
            _ = drop_null_struct_series(series)


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
    expected: ClassVar[Series] = Series([
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
    ])

    def test_expr(self) -> None:
        data = datetime_range(self.start, self.end, interval="10s")
        result = collect_series(floor_datetime(data, "1m"))
        assert_series_equal(result, self.expected, check_names=False)

    def test_series(self) -> None:
        data = datetime_range(self.start, self.end, interval="10s", eager=True)
        result = floor_datetime(data, "1m")
        assert_series_equal(result, self.expected, check_names=False)


class TestIsNullAndIsNotNullStructSeries:
    @mark.parametrize(
        ("func", "exp_values"),
        [
            param(is_null_struct_series, [True, False, False, False]),
            param(is_not_null_struct_series, [False, True, True, True]),
        ],
    )
    def test_main(
        self, *, func: Callable[[Series], Series], exp_values: list[bool]
    ) -> None:
        series = Series(
            values=[
                {"a": None, "b": None},
                {"a": True, "b": None},
                {"a": None, "b": False},
                {"a": True, "b": False},
            ],
            dtype=Struct({"a": Boolean, "b": Boolean}),
        )
        result = func(series)
        expected = Series(exp_values, dtype=Boolean)
        assert_series_equal(result, expected)

    @mark.parametrize(
        ("func", "exp_values"),
        [
            param(is_null_struct_series, [False, False, False, True]),
            param(is_not_null_struct_series, [True, True, True, False]),
        ],
    )
    def test_nested(
        self, *, func: Callable[[Series], Series], exp_values: list[bool]
    ) -> None:
        series = Series(
            values=[
                {"a": 1, "b": 2, "inner": {"lower": 3, "upper": 4}},
                {"a": 1, "b": 2, "inner": None},
                {"a": None, "b": None, "inner": {"lower": 3, "upper": 4}},
                {"a": None, "b": None, "inner": None},
            ],
            dtype=Struct({
                "a": Int64,
                "b": Int64,
                "inner": Struct({"lower": Int64, "upper": Int64}),
            }),
        )
        result = func(series)
        expected = Series(exp_values, dtype=Boolean)
        assert_series_equal(result, expected)

    @mark.parametrize(
        ("func", "error"),
        [
            param(is_null_struct_series, IsNullStructSeriesError),
            param(is_not_null_struct_series, IsNotNullStructSeriesError),
        ],
    )
    def test_error(
        self, *, func: Callable[[Series], Series], error: type[Exception]
    ) -> None:
        series = Series(name="series", values=[1, 2, 3, None], dtype=Int64)
        with raises(error, match="Series must have Struct-dtype; got Int64"):
            _ = func(series)


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
        ("values", "expected"),
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
        values: list[Any],
        expected: int | None,
        dtype: PolarsDataType,
        mode: Literal["str", "column"],
    ) -> None:
        df = DataFrame(data=values, schema={"value": dtype}).with_columns(id=lit("id"))
        match mode:
            case "str":
                agg = "value"
            case "column":
                agg = col("value")
        result = df.group_by("id").agg(nan_sum_agg(agg))
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
        df = DataFrame(
            [(x, y)], schema={"x": Int64, "y": Int64}, orient="row"
        ).with_columns(z=nan_sum_cols(x_use, y_use))
        assert df["z"].item() == expected


class TestReplaceTimeZone:
    now_utc: ClassVar[dt.datetime] = dt.datetime(2000, 1, 1, 12, tzinfo=UTC)
    now_naive: ClassVar[dt.datetime] = now_utc.replace(tzinfo=None)

    def test_series_datetime(self) -> None:
        series = Series(name="series", values=[self.now_utc], dtype=DatetimeUTC)
        result = replace_time_zone(series, time_zone=None)
        expected = Series(name="series", values=[self.now_naive], dtype=Datetime)
        assert_series_equal(result, expected)

    def test_series_non_datetime(self) -> None:
        series = Series(name="series", values=[True], dtype=Boolean)
        result = replace_time_zone(series, time_zone=None)
        assert_series_equal(result, series)

    def test_series_nested(self) -> None:
        series = Series(
            name="series",
            values=[{"datetime": self.now_utc, "boolean": True}],
            dtype=Struct({"datetime": DatetimeUTC, "boolean": Boolean}),
        )
        result = replace_time_zone(series, time_zone=None)
        expected = Series(
            name="series",
            values=[{"datetime": self.now_naive, "boolean": True}],
            dtype=Struct({"datetime": Datetime, "boolean": Boolean}),
        )
        assert_series_equal(result, expected)

    def test_series_nested_twice(self) -> None:
        series = Series(
            name="series",
            values=[{"datetime": {"inner": self.now_utc}, "boolean": True}],
            dtype=Struct({
                "datetime": Struct({"inner": DatetimeUTC}),
                "boolean": Boolean,
            }),
        )
        result = replace_time_zone(series, time_zone=None)
        expected = Series(
            name="series",
            values=[{"datetime": {"inner": self.now_naive}, "boolean": True}],
            dtype=Struct({"datetime": Struct({"inner": Datetime}), "boolean": Boolean}),
        )
        assert_series_equal(result, expected)

    def test_dataframe_datetime(self) -> None:
        df = DataFrame(data=[self.now_utc], schema={"datetime": DatetimeUTC})
        result = replace_time_zone(df, time_zone=None)
        expected = DataFrame(data=[self.now_naive], schema={"datetime": Datetime})
        assert_frame_equal(result, expected)

    def test_dataframe_non_datetime(self) -> None:
        df = DataFrame(data=[True], schema={"boolean": Boolean})
        result = replace_time_zone(df, time_zone=None)
        expected = DataFrame(data=[True], schema={"boolean": Boolean})
        assert_frame_equal(result, expected)

    def test_dataframe_nested(self) -> None:
        df = DataFrame(
            data=[(self.now_utc, True)],
            schema={"datetime": DatetimeUTC, "boolean": Boolean},
            orient="row",
        )
        result = replace_time_zone(df, time_zone=None)
        expected = DataFrame(
            data=[(self.now_naive, True)],
            schema={"datetime": Datetime, "boolean": Boolean},
            orient="row",
        )
        assert_frame_equal(result, expected)

    def test_dataframe_nested_twice(self) -> None:
        df = DataFrame(
            data=[((self.now_utc,), True)],
            schema={"datetime": Struct({"inner": DatetimeUTC}), "boolean": Boolean},
            orient="row",
        )
        result = replace_time_zone(df, time_zone=None)
        expected = DataFrame(
            data=[((self.now_naive,), True)],
            schema={"datetime": Struct({"inner": Datetime}), "boolean": Boolean},
            orient="row",
        )
        assert_frame_equal(result, expected)


class TestSetFirstRowAsColumns:
    def test_empty(self) -> None:
        df = DataFrame()
        with raises(SetFirstRowAsColumnsError):
            _ = set_first_row_as_columns(df)

    def test_one_row(self) -> None:
        df = DataFrame(["value"])
        check_polars_dataframe(df, height=1, schema_list={"column_0": Utf8})
        result = set_first_row_as_columns(df)
        check_polars_dataframe(result, height=0, schema_list={"value": Utf8})

    def test_multiple_rows(self) -> None:
        df = DataFrame(["foo", "bar", "baz"])
        check_polars_dataframe(df, height=3, schema_list={"column_0": Utf8})
        result = set_first_row_as_columns(df)
        check_polars_dataframe(result, height=2, schema_list={"foo": Utf8})


class TestStructDataType:
    def test_simple(self) -> None:
        @dataclass(kw_only=True)
        class Example:
            bool_: bool
            bool_maybe: bool | None = None
            date: dt.date
            date_maybe: dt.date | None = None
            float_: float
            float_maybe: float | None = None
            int_: int
            int_maybe: int | None = None
            str_: str
            str_maybe: str | None = None

        result = struct_data_type(Example)
        expected = Struct({
            "bool_": Boolean,
            "bool_maybe": Boolean,
            "date": Date,
            "date_maybe": Date,
            "float_": Float64,
            "float_maybe": Float64,
            "int_": Int64,
            "int_maybe": Int64,
            "str_": Utf8,
            "str_maybe": Utf8,
        })
        assert result == expected

    def test_datetime(self) -> None:
        @dataclass(kw_only=True)
        class Example:
            field: dt.datetime

        result = struct_data_type(Example, time_zone=UTC)
        expected = Struct({"field": DatetimeUTC})
        assert result == expected

    def test_enum(self) -> None:
        class Truth(Enum):
            true = auto()
            false = auto()

        @dataclass(kw_only=True)
        class Example:
            field: Truth

        result = struct_data_type(Example)
        expected = Struct({"field": Utf8})
        assert result == expected

    def test_literal(self) -> None:
        LowOrHigh = Literal["low", "high"]  # noqa: N806

        @dataclass(kw_only=True)
        class Example:
            field: LowOrHigh  # pyright: ignore[reportInvalidTypeForm]

        result = struct_data_type(Example)
        expected = Struct({"field": Utf8})
        assert result == expected

    def test_containers(self) -> None:
        @dataclass(kw_only=True)
        class Example:
            frozenset_: frozenset[int]
            list_: list[int]
            set_: set[int]

        result = struct_data_type(Example, time_zone=UTC)
        expected = Struct({
            "frozenset_": List(Int64),
            "list_": List(Int64),
            "set_": List(Int64),
        })
        assert result == expected

    def test_list_of_struct(self) -> None:
        @dataclass(kw_only=True)
        class Inner:
            field: int

        @dataclass(kw_only=True)
        class Outer:
            inner: list[Inner]

        result = struct_data_type(Outer, time_zone=UTC)
        expected = Struct({"inner": List(Struct({"field": Int64}))})
        assert result == expected

    def test_struct(self) -> None:
        @dataclass(kw_only=True)
        class Inner:
            field: int

        @dataclass(kw_only=True)
        class Outer:
            inner: Inner

        result = struct_data_type(Outer, time_zone=UTC)
        expected = Struct({"inner": Struct({"field": Int64})})
        assert result == expected

    def test_struct_of_list(self) -> None:
        @dataclass(kw_only=True)
        class Inner:
            field: list[int]

        @dataclass(kw_only=True)
        class Outer:
            inner: Inner

        result = struct_data_type(Outer, time_zone=UTC)
        expected = Struct({"inner": Struct({"field": List(Int64)})})
        assert result == expected

    def test_not_a_dataclass_error(self) -> None:
        with raises(StructDataTypeError, match="Object must be a dataclass; got None"):
            _ = struct_data_type(cast(Any, None))

    def test_missing_time_zone_error(self) -> None:
        @dataclass(kw_only=True)
        class Example:
            field: dt.datetime

        with raises(StructDataTypeError, match="Time-zone must be given"):
            _ = struct_data_type(Example)

    def test_missing_type_error(self) -> None:
        @dataclass(kw_only=True)
        class Example:
            field: None

        with raises(StructDataTypeError, match="Unsupported type: <class 'NoneType'>"):
            _ = struct_data_type(Example)


class TestYieldStructSeriesDataclasses:
    def test_main(self) -> None:
        @dataclass(kw_only=True)
        class Row:
            lower: int | None = None
            upper: int | None = None

        series = Series(
            name="series",
            values=[(1, 1), (2, None), (None, 3), (None, None)],
            dtype=Struct({"lower": Int64, "upper": Int64}),
        )
        result = list(yield_struct_series_dataclasses(series, Row))
        expected = [
            Row(lower=1, upper=1),
            Row(lower=2, upper=None),
            Row(lower=None, upper=3),
            None,
        ]
        assert result == expected

    def test_nested(self) -> None:
        @dataclass(kw_only=True)
        class Inner:
            lower: int
            upper: int

        @dataclass(kw_only=True)
        class Outer:
            a: int | None = None
            b: int | None = None
            inner: Inner | None = None

        series = Series(
            name="series",
            values=[
                {"a": 1, "b": 2, "inner": {"lower": 3, "upper": 4}},
                {"a": 1, "b": 2, "inner": None},
                {"a": None, "b": None, "inner": {"lower": 3, "upper": 4}},
                {"a": None, "b": None, "inner": None},
            ],
            dtype=Struct({
                "a": Int64,
                "b": Int64,
                "inner": Struct({"lower": Int64, "upper": Int64}),
            }),
        )
        result = list(yield_struct_series_dataclasses(series, Outer))
        expected = [
            Outer(a=1, b=2, inner=Inner(lower=3, upper=4)),
            Outer(a=1, b=2, inner=None),
            Outer(a=None, b=None, inner=Inner(lower=3, upper=4)),
            None,
        ]
        assert result == expected


class TestYieldStructSeriesElements:
    def test_main(self) -> None:
        series = Series(
            name="series",
            values=[(1, 1), (2, None), (None, 3), (None, None)],
            dtype=Struct({"lower": Int64, "upper": Int64}),
        )
        result = list(yield_struct_series_elements(series))
        expected = [
            {"lower": 1, "upper": 1},
            {"lower": 2, "upper": None},
            {"lower": None, "upper": 3},
            None,
        ]
        assert result == expected

    def test_nested(self) -> None:
        series = Series(
            name="series",
            values=[
                {"a": 1, "b": 2, "inner": {"lower": 3, "upper": 4}},
                {"a": 1, "b": 2, "inner": None},
                {"a": None, "b": None, "inner": {"lower": 3, "upper": 4}},
                {"a": None, "b": None, "inner": None},
            ],
            dtype=Struct({
                "a": Int64,
                "b": Int64,
                "inner": Struct({"lower": Int64, "upper": Int64}),
            }),
        )
        result = list(yield_struct_series_elements(series))
        expected = [
            {"a": 1, "b": 2, "inner": {"lower": 3, "upper": 4}},
            {"a": 1, "b": 2, "inner": None},
            {"a": None, "b": None, "inner": {"lower": 3, "upper": 4}},
            None,
        ]
        assert result == expected

    @mark.parametrize(
        ("obj", "expected"),
        [
            param(None, None),
            param(1, 1),
            param({"a": 1, "b": 2}, {"a": 1, "b": 2}),
            param({"a": 1, "b": None}, {"a": 1, "b": None}),
            param({"a": None, "b": None}, None),
            param(
                {"a": 1, "b": 2, "inner": {"lower": 3, "upper": 4}},
                {"a": 1, "b": 2, "inner": {"lower": 3, "upper": 4}},
            ),
            param(
                {"a": 1, "b": 2, "inner": {"lower": None, "upper": None}},
                {"a": 1, "b": 2, "inner": None},
            ),
            param(
                {"a": None, "b": None, "inner": {"lower": 3, "upper": 4}},
                {"a": None, "b": None, "inner": {"lower": 3, "upper": 4}},
            ),
            param(
                {"a": None, "b": None, "inner": {"lower": None, "upper": None}}, None
            ),
        ],
    )
    def test_remove_nulls(self, *, obj: Any, expected: Any) -> None:
        result = _yield_struct_series_element_remove_nulls(obj)
        assert result == expected

    def test_error_struct_dtype(self) -> None:
        series = Series(name="series", values=[1, 2, 3, None], dtype=Int64)
        with raises(
            YieldStructSeriesElementsError,
            match="Series must have Struct-dtype; got Int64",
        ):
            _ = list(yield_struct_series_elements(series))

    def test_error_null_elements(self) -> None:
        series = Series(
            name="series",
            values=[{"value": 1}, {"value": 2}, {"value": 3}, None],
            dtype=Struct({"value": Int64}),
        )
        with raises(
            YieldStructSeriesElementsError, match="Series must not have nulls; got .*"
        ):
            _ = list(yield_struct_series_elements(series, strict=True))


class TestZonedDatetime:
    @given(time_zone=sampled_from([HongKong, UTC]))
    def test_main(self, *, time_zone: ZoneInfo) -> None:
        dtype = zoned_datetime(time_zone=time_zone)
        assert isinstance(dtype, Datetime)
        assert dtype.time_zone is not None
