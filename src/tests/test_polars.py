from __future__ import annotations

import datetime as dt
import enum
import itertools
from dataclasses import dataclass, field
from enum import auto
from itertools import chain, repeat
from math import isfinite, nan
from pathlib import Path
from typing import TYPE_CHECKING, Any, ClassVar, Literal, cast
from uuid import UUID, uuid4

import hypothesis.strategies
import numpy as np
import polars as pl
from hypothesis import given
from hypothesis.strategies import (
    DataObject,
    SearchStrategy,
    booleans,
    builds,
    data,
    fixed_dictionaries,
    floats,
    lists,
    none,
    sampled_from,
    timezones,
)
from numpy import allclose, linspace, pi
from polars import (
    Boolean,
    DataFrame,
    DataType,
    Date,
    Datetime,
    Expr,
    Float64,
    Int32,
    Int64,
    List,
    Object,
    Series,
    String,
    Struct,
    UInt32,
    col,
    concat,
    date_range,
    datetime_range,
    int_range,
    lit,
    struct,
)
from polars._typing import IntoExprColumn, SchemaDict
from polars.exceptions import ComputeError
from polars.schema import Schema
from polars.testing import assert_frame_equal, assert_series_equal
from pytest import mark, param, raises
from whenever import TimeZoneNotFoundError

import utilities.polars
from utilities.hypothesis import (
    assume_does_not_raise,
    float64s,
    int64s,
    pairs,
    py_datetimes,
    temp_paths,
    text_ascii,
    zoned_datetimes,
)
from utilities.math import number_of_decimals
from utilities.numpy import DEFAULT_RNG
from utilities.pathlib import PWD
from utilities.polars import (
    AppendDataClassError,
    BooleanValueCountsError,
    ColumnsToDictError,
    DatetimeHongKong,
    DatetimeTokyo,
    DatetimeUSCentral,
    DatetimeUSEastern,
    DatetimeUTC,
    DropNullStructSeriesError,
    FiniteEWMMeanError,
    InsertAfterError,
    InsertBeforeError,
    IsNotNullStructSeriesError,
    IsNullStructSeriesError,
    SetFirstRowAsColumnsError,
    StructFromDataClassError,
    _check_polars_dataframe_predicates,
    _check_polars_dataframe_schema_list,
    _check_polars_dataframe_schema_set,
    _check_polars_dataframe_schema_subset,
    _CheckPolarsDataFrameColumnsError,
    _CheckPolarsDataFrameDTypesError,
    _CheckPolarsDataFrameHeightError,
    _CheckPolarsDataFramePredicatesError,
    _CheckPolarsDataFrameSchemaListError,
    _CheckPolarsDataFrameSchemaSetError,
    _CheckPolarsDataFrameSchemaSubsetError,
    _CheckPolarsDataFrameShapeError,
    _CheckPolarsDataFrameSortedError,
    _CheckPolarsDataFrameUniqueError,
    _CheckPolarsDataFrameWidthError,
    _DataClassToDataFrameEmptyError,
    _DataClassToDataFrameNonUniqueError,
    _deconstruct_dtype,
    _deconstruct_schema,
    _finite_ewm_weights,
    _FiniteEWMWeightsError,
    _GetDataTypeOrSeriesTimeZoneNotDateTimeError,
    _GetDataTypeOrSeriesTimeZoneNotZonedError,
    _GetSeriesNumberOfDecimalsAllNullError,
    _GetSeriesNumberOfDecimalsNotFloatError,
    _InsertBetweenMissingColumnsError,
    _InsertBetweenNonConsecutiveError,
    _IsNearEventAfterError,
    _IsNearEventBeforeError,
    _JoinIntoPeriodsArgumentsError,
    _JoinIntoPeriodsOverlappingError,
    _JoinIntoPeriodsPeriodError,
    _JoinIntoPeriodsSortedError,
    _reconstruct_dtype,
    _reconstruct_schema,
    _ReifyExprsEmptyError,
    _ReifyExprsSeriesNonUniqueError,
    ac_halflife,
    acf,
    adjust_frequencies,
    append_dataclass,
    are_frames_equal,
    bernoulli,
    boolean_value_counts,
    ceil_datetime,
    check_polars_dataframe,
    choice,
    collect_series,
    columns_to_dict,
    concat_series,
    convert_time_zone,
    cross,
    cross_rolling_quantile,
    dataclass_to_dataframe,
    dataclass_to_schema,
    deserialize_dataframe,
    deserialize_series,
    drop_null_struct_series,
    ensure_data_type,
    ensure_expr_or_series,
    ensure_expr_or_series_many,
    finite_ewm_mean,
    floor_datetime,
    get_data_type_or_series_time_zone,
    get_expr_name,
    get_frequency_spectrum,
    get_series_number_of_decimals,
    insert_after,
    insert_before,
    insert_between,
    is_near_event,
    is_not_null_struct_series,
    is_null_struct_series,
    join,
    join_into_periods,
    map_over_columns,
    nan_sum_agg,
    nan_sum_cols,
    normal,
    order_of_magnitude,
    read_dataframe,
    read_series,
    reify_exprs,
    replace_time_zone,
    serialize_dataframe,
    serialize_series,
    set_first_row_as_columns,
    struct_dtype,
    struct_from_dataclass,
    touch,
    try_reify_expr,
    uniform,
    unique_element,
    week_num,
    write_dataframe,
    write_series,
    zoned_datetime,
)
from utilities.random import get_state
from utilities.sentinel import Sentinel, sentinel
from utilities.tzdata import HongKong, Tokyo, USCentral, USEastern
from utilities.whenever import get_now, get_today
from utilities.zoneinfo import UTC, get_time_zone_name

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Mapping, Sequence
    from zoneinfo import ZoneInfo

    from polars._typing import IntoExprColumn, PolarsDataType, SchemaDict
    from polars.datatypes import DataTypeClass

    from utilities.types import MaybeType, StrMapping, WeekDay


class TestACF:
    def test_main(self) -> None:
        series = Series(linspace(0, 2 * pi, 1000))
        df = acf(series)
        check_polars_dataframe(
            df, height=31, schema_list={"lag": UInt32, "autocorrelation": Float64}
        )

    def test_alpha(self) -> None:
        series = Series(linspace(0, 2 * pi, 1000))
        df = acf(series, alpha=0.5)
        check_polars_dataframe(
            df,
            height=31,
            schema_list={
                "lag": UInt32,
                "autocorrelation": Float64,
                "lower": Float64,
                "upper": Float64,
            },
        )

    def test_qstat(self) -> None:
        series = Series(linspace(0, 2 * pi, 1000))
        df = acf(series, qstat=True)
        check_polars_dataframe(
            df,
            height=31,
            schema_list={
                "lag": UInt32,
                "autocorrelation": Float64,
                "qstat": Float64,
                "pvalue": Float64,
            },
        )

    def test_alpha_and_qstat(self) -> None:
        series = Series(linspace(0, 2 * pi, 1000))
        df = acf(series, alpha=0.5, qstat=True)
        check_polars_dataframe(
            df,
            height=31,
            schema_list={
                "lag": UInt32,
                "autocorrelation": Float64,
                "lower": Float64,
                "upper": Float64,
                "qstat": Float64,
                "pvalue": Float64,
            },
        )


class TestACHalfLife:
    def test_main(self) -> None:
        series = Series(linspace(0, 2 * pi, 1000))
        halflife = ac_halflife(series)
        assert halflife == 169.94


class TestAdjustFrequencies:
    def test_main(self) -> None:
        n = 1000
        x = linspace(0, 2 * pi, n)
        noise = DEFAULT_RNG.normal(scale=0.25, size=n)
        y = Series(values=x + noise)
        result = adjust_frequencies(y, filters=lambda f: np.abs(f) <= 0.02)
        assert isinstance(result, Series)


class TestAppendDataClass:
    @given(
        data=fixed_dictionaries({
            "a": int64s() | none(),
            "b": floats() | none(),
            "c": text_ascii() | none(),
        })
    )
    def test_columns_and_fields_equal(self, *, data: StrMapping) -> None:
        df = DataFrame(schema={"a": Int64, "b": Float64, "c": String})

        @dataclass(kw_only=True, slots=True)
        class Row:
            a: int | None = None
            b: float | None = None
            c: str | None = None

        row = Row(**data)
        result = append_dataclass(df, row)
        height = 0 if (row.a is None) and (row.b is None) and (row.c is None) else 1
        check_polars_dataframe(result, height=height, schema_list=df.schema)

    @given(data=fixed_dictionaries({"a": int64s() | none(), "b": floats() | none()}))
    def test_extra_column(self, *, data: StrMapping) -> None:
        df = DataFrame(schema={"a": Int64, "b": Float64, "c": String})

        @dataclass(kw_only=True, slots=True)
        class Row:
            a: int | None = None
            b: float | None = None

        row = Row(**data)
        result = append_dataclass(df, row)
        height = 0 if (row.a is None) and (row.b is None) else 1
        check_polars_dataframe(result, height=height, schema_list=df.schema)

    @given(data=fixed_dictionaries({"a": int64s() | none(), "b": floats() | none()}))
    def test_extra_field_but_none(self, *, data: StrMapping) -> None:
        df = DataFrame(schema={"a": Int64, "b": Float64})

        @dataclass(kw_only=True, slots=True)
        class Row:
            a: int | None = None
            b: float | None = None
            c: str | None = None

        row = Row(**data)
        result = append_dataclass(df, row)
        height = 0 if (row.a is None) and (row.b is None) else 1
        check_polars_dataframe(result, height=height, schema_list=df.schema)

    @given(
        data=fixed_dictionaries({
            "datetime": zoned_datetimes().map(lambda d: d.py_datetime())
        })
    )
    def test_zoned_datetime(self, *, data: StrMapping) -> None:
        df = DataFrame(schema={"datetime": DatetimeUTC})

        @dataclass(kw_only=True, slots=True)
        class Row:
            datetime: dt.datetime

        row = Row(**data)
        result = append_dataclass(df, row)
        check_polars_dataframe(result, height=1, schema_list=df.schema)

    @given(
        data=fixed_dictionaries({
            "a": int64s() | none(),
            "b": floats() | none(),
            "c": text_ascii(),
        })
    )
    def test_error(self, *, data: StrMapping) -> None:
        df = DataFrame(schema={"a": Int64, "b": Float64})

        @dataclass(kw_only=True, slots=True)
        class Row:
            a: int | None = None
            b: float | None = None
            c: str

        row = Row(**data)
        with raises(
            AppendDataClassError,
            match="Dataclass fields .* must be a subset of DataFrame columns .*; dataclass had extra items .*",
        ):
            _ = append_dataclass(df, row)


class TestAreFramesEqual:
    @given(
        case=sampled_from([
            (DataFrame(), DataFrame(), True),
            (DataFrame(), DataFrame(schema={"value": Int64}), False),
        ])
    )
    def test_main(self, *, case: tuple[DataFrame, DataFrame, bool]) -> None:
        x, y, expected = case
        result = are_frames_equal(x, y)
        assert result is expected


class TestBernoulli:
    @given(length=hypothesis.strategies.integers(0, 10))
    def test_int(self, *, length: int) -> None:
        series = bernoulli(length)
        self._assert(series, length)

    @given(length=hypothesis.strategies.integers(0, 10))
    def test_series(self, *, length: int) -> None:
        orig = int_range(end=length, eager=True)
        series = bernoulli(orig)
        self._assert(series, length)

    @given(length=hypothesis.strategies.integers(0, 10))
    def test_dataframe(self, *, length: int) -> None:
        df = int_range(end=length, eager=True).to_frame()
        series = bernoulli(df)
        self._assert(series, length)

    def _assert(self, series: Series, length: int, /) -> None:
        assert series.dtype == Boolean
        assert series.len() == length
        assert series.is_not_null().all()


class TestBooleanValueCounts:
    df: ClassVar[DataFrame] = DataFrame(
        data=[
            (False, False),
            (True, None),
            (True, True),
            (True, None),
            (False, True),
            (None, True),
            (False, False),
            (False, True),
            (False, False),
            (None, True),
        ],
        schema={"x": Boolean, "y": Boolean},
        orient="row",
    )
    schema: ClassVar[SchemaDict] = {
        "name": String,
        "true": UInt32,
        "false": UInt32,
        "null": UInt32,
        "total": UInt32,
        "true (%)": Float64,
        "false (%)": Float64,
        "null (%)": Float64,
    }

    def test_series(self) -> None:
        result = boolean_value_counts(self.df["x"], "x")
        check_polars_dataframe(result, height=1, schema_list=self.schema)

    def test_dataframe(self) -> None:
        result = boolean_value_counts(
            self.df,
            "x",
            "y",
            (col("x") & col("y")).alias("x_and_y"),
            x_or_y=col("x") | col("y"),
        )
        check_polars_dataframe(result, height=4, schema_list=self.schema)

    def test_empty(self) -> None:
        result = boolean_value_counts(self.df[:0], "x")
        check_polars_dataframe(result, height=1, schema_list=self.schema)
        for column in ["true", "false", "null", "total"]:
            assert (result[column] == 0).all()
        for column in ["true (%)", "false (%)", "null (%)"]:
            assert result[column].is_nan().all()

    def test_error(self) -> None:
        with raises(
            BooleanValueCountsError, match="Column 'z' must be Boolean; got Int64"
        ):
            _ = boolean_value_counts(self.df, col("x").cast(Int64).alias("z"))


class TestCeilDateTime:
    start: ClassVar[dt.datetime] = dt.datetime(2000, 1, 1, 0, 0, tzinfo=UTC)
    end: ClassVar[dt.datetime] = dt.datetime(2000, 1, 1, 0, 3, tzinfo=UTC)
    expected: ClassVar[Series] = Series(
        values=[
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
        df = DataFrame()
        check_polars_dataframe(df)

    def test_columns_pass(self) -> None:
        df = DataFrame()
        check_polars_dataframe(df, columns=[])

    def test_columns_error(self) -> None:
        df = DataFrame()
        with raises(
            _CheckPolarsDataFrameColumnsError,
            match="DataFrame must have columns .*; got .*:\n\n.*",
        ):
            check_polars_dataframe(df, columns=["value"])

    def test_dtypes_pass(self) -> None:
        df = DataFrame()
        check_polars_dataframe(df, dtypes=[])

    def test_dtypes_error(self) -> None:
        df = DataFrame()
        with raises(
            _CheckPolarsDataFrameDTypesError,
            match="DataFrame must have dtypes .*; got .*\n\n.*",
        ):
            check_polars_dataframe(df, dtypes=[Float64])

    def test_height_pass(self) -> None:
        df = DataFrame(data={"value": [0.0]})
        check_polars_dataframe(df, height=1)

    def test_height_error(self) -> None:
        df = DataFrame(data={"value": [0.0]})
        with raises(
            _CheckPolarsDataFrameHeightError,
            match="DataFrame must satisfy the height requirements; got .*:\n\n.*",
        ):
            check_polars_dataframe(df, height=2)

    def test_min_height_pass(self) -> None:
        df = DataFrame(data={"value": [0.0, 1.0]})
        check_polars_dataframe(df, min_height=1)

    def test_min_height_error(self) -> None:
        df = DataFrame()
        with raises(
            _CheckPolarsDataFrameHeightError,
            match="DataFrame must satisfy the height requirements; got .*:\n\n.*",
        ):
            check_polars_dataframe(df, min_height=1)

    def test_max_height_pass(self) -> None:
        df = DataFrame()
        check_polars_dataframe(df, max_height=1)

    def test_max_height_error(self) -> None:
        df = DataFrame(data={"value": [0.0, 1.0]})
        with raises(
            _CheckPolarsDataFrameHeightError,
            match="DataFrame must satisfy the height requirements; got .*:\n\n.*",
        ):
            check_polars_dataframe(df, max_height=1)

    def test_predicates_pass(self) -> None:
        df = DataFrame(data={"value": [0.0, 1.0]})
        check_polars_dataframe(df, predicates={"value": isfinite})

    def test_predicates_error_missing_columns_and_failed(self) -> None:
        df = DataFrame(data={"a": [0.0, nan], "b": [0.0, nan]})
        with raises(
            _CheckPolarsDataFramePredicatesError,
            match="DataFrame must satisfy the predicates; missing columns were .* and failed predicates were .*:\n\n.*",
        ):
            check_polars_dataframe(df, predicates={"a": isfinite, "c": isfinite})

    def test_predicates_error_missing_columns_only(self) -> None:
        df = DataFrame()
        with raises(
            _CheckPolarsDataFramePredicatesError,
            match="DataFrame must satisfy the predicates; missing columns were .*:\n\n.*",
        ):
            check_polars_dataframe(df, predicates={"a": isfinite})

    def test_predicates_error_failed_only(self) -> None:
        df = DataFrame(data={"a": [0.0, nan]})
        with raises(
            _CheckPolarsDataFramePredicatesError,
            match="DataFrame must satisfy the predicates; failed predicates were .*:\n\n.*",
        ):
            check_polars_dataframe(df, predicates={"a": isfinite})

    def test_schema_list_pass(self) -> None:
        df = DataFrame()
        check_polars_dataframe(df, schema_list={})

    def test_schema_list_error_set_of_columns(self) -> None:
        df = DataFrame()
        with raises(
            _CheckPolarsDataFrameSchemaListError,
            match=r"DataFrame must have schema .* \(ordered\); got .*:\n\n.*",
        ):
            check_polars_dataframe(df, schema_list={"value": Float64})

    def test_schema_list_error_order_of_columns(self) -> None:
        df = DataFrame(schema={"a": Float64, "b": Float64})
        with raises(
            _CheckPolarsDataFrameSchemaListError,
            match=r"DataFrame must have schema .* \(ordered\); got .*:\n\n.*",
        ):
            check_polars_dataframe(df, schema_list={"b": Float64, "a": Float64})

    def test_schema_set_pass(self) -> None:
        df = DataFrame(schema={"a": Float64, "b": Float64})
        check_polars_dataframe(df, schema_set={"b": Float64, "a": Float64})

    def test_schema_set_error_set_of_columns(self) -> None:
        df = DataFrame()
        with raises(
            _CheckPolarsDataFrameSchemaSetError,
            match=r"DataFrame must have schema .* \(unordered\); got .*:\n\n.*",
        ):
            check_polars_dataframe(df, schema_set={"value": Float64})

    def test_schema_subset_pass(self) -> None:
        df = DataFrame(data={"foo": [0.0], "bar": [0.0]})
        check_polars_dataframe(df, schema_subset={"foo": Float64})

    def test_schema_subset_error(self) -> None:
        df = DataFrame(data={"foo": [0.0]})
        with raises(
            _CheckPolarsDataFrameSchemaSubsetError,
            match=r"DataFrame schema must include .* \(unordered\); got .*:\n\n.*",
        ):
            check_polars_dataframe(df, schema_subset={"bar": Float64})

    def test_shape_pass(self) -> None:
        df = DataFrame()
        check_polars_dataframe(df, shape=(0, 0))

    def test_shape_error(self) -> None:
        df = DataFrame()
        with raises(
            _CheckPolarsDataFrameShapeError,
            match="DataFrame must have shape .*; got .*:\n\n.*",
        ):
            check_polars_dataframe(df, shape=(1, 1))

    def test_sorted_pass(self) -> None:
        df = DataFrame(data={"value": [0.0, 1.0]})
        check_polars_dataframe(df, sorted="value")

    def test_sorted_error(self) -> None:
        df = DataFrame(data={"value": [1.0, 0.0]})
        with raises(
            _CheckPolarsDataFrameSortedError,
            match="DataFrame must be sorted on .*:\n\n.*",
        ):
            check_polars_dataframe(df, sorted="value")

    def test_unique_pass(self) -> None:
        df = DataFrame(data={"value": [0.0, 1.0]})
        check_polars_dataframe(df, unique="value")

    def test_unique_error(self) -> None:
        df = DataFrame(data={"value": [0.0, 0.0]})
        with raises(
            _CheckPolarsDataFrameUniqueError,
            match="DataFrame must be unique on .*:\n\n.*",
        ):
            check_polars_dataframe(df, unique="value")

    def test_width_pass(self) -> None:
        df = DataFrame()
        check_polars_dataframe(df, width=0)

    def test_width_error(self) -> None:
        df = DataFrame()
        with raises(
            _CheckPolarsDataFrameWidthError,
            match="DataFrame must have width .*; got .*:\n\n.*",
        ):
            check_polars_dataframe(df, width=1)


class TestCheckPolarsDataFramePredicates:
    def test_pass(self) -> None:
        df = DataFrame(data={"value": [0.0, 1.0]})
        _check_polars_dataframe_predicates(df, {"value": isfinite})

    @given(
        predicates=sampled_from([
            {"other": Float64},  # missing column
            {"value": isfinite},  # failed
        ])
    )
    def test_error(self, *, predicates: Mapping[str, Callable[[Any], bool]]) -> None:
        df = DataFrame(data={"value": [0.0, nan]})
        with raises(
            _CheckPolarsDataFramePredicatesError,
            match="DataFrame must satisfy the predicates; (missing columns|failed predicates) were .*:\n\n.*",
        ):
            _check_polars_dataframe_predicates(df, predicates)


class TestCheckPolarsDataFrameSchemaList:
    def test_pass(self) -> None:
        df = DataFrame(data={"value": [0.0]})
        _check_polars_dataframe_schema_list(df, {"value": Float64})

    def test_error(self) -> None:
        df = DataFrame()
        with raises(
            _CheckPolarsDataFrameSchemaListError,
            match=r"DataFrame must have schema .* \(ordered\); got .*:\n\n.*",
        ):
            _check_polars_dataframe_schema_list(df, {"value": Float64})


class TestCheckPolarsDataFrameSchemaSet:
    def test_pass(self) -> None:
        df = DataFrame(data={"foo": [0.0], "bar": [0.0]})
        _check_polars_dataframe_schema_set(df, {"bar": Float64, "foo": Float64})

    def test_error(self) -> None:
        df = DataFrame()
        with raises(
            _CheckPolarsDataFrameSchemaSetError,
            match=r"DataFrame must have schema .* \(unordered\); got .*:\n\n.*",
        ):
            _check_polars_dataframe_schema_set(df, {"value": Float64})


class TestCheckPolarsDataFrameSchemaSubset:
    def test_pass(self) -> None:
        df = DataFrame(data={"foo": [0.0], "bar": [0.0]})
        _check_polars_dataframe_schema_subset(df, {"foo": Float64})

    @given(
        schema_inc=sampled_from([
            {"bar": Float64},  #  missing column
            {"foo": Int64},  #  wrong dtype
        ])
    )
    def test_error(self, *, schema_inc: SchemaDict) -> None:
        df = DataFrame(data={"foo": [0.0]})
        with raises(
            _CheckPolarsDataFrameSchemaSubsetError,
            match=r"DataFrame schema must include .* \(unordered\); got .*:\n\n.*",
        ):
            _check_polars_dataframe_schema_subset(df, schema_inc)


class TestChoice:
    @given(length=hypothesis.strategies.integers(0, 10))
    def test_int_with_bool(self, *, length: int) -> None:
        elements = [True, False, None]
        series = choice(length, elements, dtype=Boolean)
        self._assert(series, length, elements, dtype=Boolean)

    @given(length=hypothesis.strategies.integers(0, 10))
    def test_int_with_str(self, *, length: int) -> None:
        elements = ["A", "B", "C"]
        series = choice(length, elements, dtype=String)
        self._assert(series, length, elements, dtype=String)

    @given(length=hypothesis.strategies.integers(0, 10))
    def test_series(self, *, length: int) -> None:
        orig = int_range(end=length, eager=True)
        elements = ["A", "B", "C"]
        series = choice(orig, elements, dtype=String)
        self._assert(series, length, elements, dtype=String)

    @given(length=hypothesis.strategies.integers(0, 10))
    def test_dataframe(self, *, length: int) -> None:
        df = int_range(end=length, eager=True).to_frame()
        elements = ["A", "B", "C"]
        series = choice(df, elements, dtype=String)
        self._assert(series, length, elements, dtype=String)

    def _assert(
        self,
        series: Series,
        length: int,
        elements: Iterable[Any],
        /,
        *,
        dtype: PolarsDataType = Float64,
    ) -> None:
        assert series.dtype == dtype
        assert series.len() == length
        assert series.is_in(list(elements)).all()


class TestCollectSeries:
    def test_main(self) -> None:
        expr = int_range(end=10)
        series = collect_series(expr)
        expected = int_range(end=10, eager=True)
        assert_series_equal(series, expected)


class TestColumnsToDict:
    def test_main(self) -> None:
        df = DataFrame(
            data=[{"a": 1, "b": 11}, {"a": 2, "b": 12}, {"a": 3, "b": 13}],
            schema={"a": Int64, "b": Int64},
        )
        mapping = columns_to_dict(df, "a", "b")
        assert mapping == {1: 11, 2: 12, 3: 13}

    def test_error(self) -> None:
        df = DataFrame(
            data=[{"a": 1, "b": 11}, {"a": 2, "b": 12}, {"a": 1, "b": 13}],
            schema={"a": Int64, "b": Int64},
        )
        with raises(ColumnsToDictError, match="DataFrame must be unique on 'a':\n\n.*"):
            _ = columns_to_dict(df, "a", "b")


class TestConcatSeries:
    def test_main(self) -> None:
        x, y = [
            Series(name=n, values=[v], dtype=Boolean)
            for n, v in [("x", True), ("y", False)]
        ]
        df = concat_series(x, y)
        expected = DataFrame(
            [(True, False)], schema={"x": Boolean, "y": Boolean}, orient="row"
        )
        assert_frame_equal(df, expected)


class TestConvertTimeZone:
    def test_datetime(self) -> None:
        now = get_now().py_datetime()
        series = Series(values=[now], dtype=DatetimeUTC)
        result = convert_time_zone(series, time_zone=HongKong)
        expected = Series(values=[now.astimezone(HongKong)], dtype=DatetimeHongKong)
        assert_series_equal(result, expected)

    def test_non_datetime(self) -> None:
        series = Series(values=[True], dtype=Boolean)
        result = convert_time_zone(series, time_zone=HongKong)
        assert_series_equal(result, series)


class TestCrossOrTouch:
    @given(
        case=sampled_from([
            ("cross", "x", "up", [None, False, False, False, True, False, False]),
            ("cross", "y", "down", [None, False, False, False, True, False, False]),
            ("touch", "x", "up", [None, False, False, True, False, False, False]),
            ("touch", "y", "down", [None, False, False, True, False, False, False]),
        ]),
        data=data(),
        other=sampled_from([3, "z"]),
    )
    def test_main(
        self,
        *,
        case: tuple[
            Literal["cross", "touch"],
            Literal["x", "y"],
            Literal["up", "down"],
            list[bool | None],
        ],
        data: DataObject,
        other: Literal[3, "z"],
    ) -> None:
        cross_or_touch, column, up_or_down, exp_values = case
        df = concat_series(
            int_range(0, 7, eager=True).alias("x"),
            int_range(6, -1, -1, eager=True).alias("y"),
            pl.repeat(3, 7, eager=True).alias("z"),
        )
        expr = data.draw(sampled_from([column, df[column]]))
        match other:
            case 3:
                other_use = other
            case str():
                other_use = data.draw(sampled_from([other, df[other]]))
        match cross_or_touch:
            case "cross":
                result = cross(expr, up_or_down, other_use)
            case "touch":
                result = touch(expr, up_or_down, other_use)
        df = df.with_columns(result.alias("result"))
        expected = Series(name="result", values=exp_values, dtype=Boolean)
        assert_series_equal(df["result"], expected)

    def test_example(self) -> None:
        close = Series(name="close", values=[8, 7, 8, 5, 0], dtype=Int64)
        mid = Series(name="mid", values=[1, 2, 3, 4, 6], dtype=Int64)
        result = cross(close, "down", mid).alias("result")
        expected = Series(
            name="result", values=[None, False, False, False, True], dtype=Boolean
        )
        assert_series_equal(result, expected)


class TestCrossRollingQuantile:
    def test_main(self) -> None:
        df = DataFrame(
            data=[
                (4, None, None),
                (5, None, None),
                (7, None, None),
                (9, None, None),
                (0, 5.0, False),
                (1, 5.0, False),
                (8, 7.0, True),
                (9, 8.0, False),
                (2, 2.0, False),
                (3, 3.0, False),
            ],
            schema={"x": Int64, "median": Float64, "cross": Boolean},
            orient="row",
        )
        assert_series_equal(
            df["x"].rolling_quantile(0.5, window_size=5),
            df["median"],
            check_names=False,
        )
        assert_series_equal(
            cross_rolling_quantile(df["x"], "up", 0.5, window_size=5),
            df["cross"],
            check_names=False,
        )

    def test_example(self) -> None:
        close = Series(name="close", values=[8, 7, 8, 5, 0], dtype=Int64)
        mid = Series(name="mid", values=[1, 2, 3, 4, 6], dtype=Int64)
        result = cross(close, "down", mid).alias("result")
        expected = Series(
            name="result", values=[None, False, False, False, True], dtype=Boolean
        )
        assert_series_equal(result, expected)


class TestDataClassToDataFrame:
    @given(data=data())
    def test_basic_type(self, *, data: DataObject) -> None:
        @dataclass(kw_only=True, slots=True)
        class Example:
            bool_field: bool
            int_field: int
            float_field: float
            str_field: str
            date_field: dt.date

        objs = data.draw(lists(builds(Example, int_field=int64s()), min_size=1))
        df = dataclass_to_dataframe(objs)
        check_polars_dataframe(
            df,
            height=len(objs),
            schema_list={
                "bool_field": Boolean,
                "int_field": Int64,
                "float_field": Float64,
                "str_field": String,
                "date_field": Date,
            },
        )

    @given(data=data())
    def test_nested(self, *, data: DataObject) -> None:
        @dataclass(kw_only=True, slots=True)
        class Inner:
            x: int = 0

        @dataclass(kw_only=True, slots=True)
        class Outer:
            inner: Inner = field(default_factory=Inner)

        objs = data.draw(lists(builds(Outer), min_size=1))
        df = dataclass_to_dataframe(objs, localns=locals())
        check_polars_dataframe(
            df, height=len(objs), schema_list={"inner": struct_dtype(x=Int64)}
        )

    @given(data=data())
    def test_path(self, *, data: DataObject) -> None:
        @dataclass(kw_only=True, slots=True)
        class Example:
            x: Path = PWD

        obj = data.draw(builds(Example))
        df = dataclass_to_dataframe(obj, localns=locals())
        check_polars_dataframe(df, height=len(df), schema_list={"x": String})

    @given(data=data())
    def test_uuid(self, *, data: DataObject) -> None:
        @dataclass(kw_only=True, slots=True)
        class Example:
            x: UUID = field(default_factory=uuid4)

        obj = data.draw(builds(Example))
        df = dataclass_to_dataframe(obj, localns=locals())
        check_polars_dataframe(df, height=len(df), schema_list={"x": String})

    @given(data=data(), time_zone=timezones())
    def test_zoned_datetime(self, *, data: DataObject, time_zone: ZoneInfo) -> None:
        @dataclass(kw_only=True, slots=True)
        class Example:
            x: dt.datetime

        objs = data.draw(
            lists(
                builds(
                    Example,
                    x=zoned_datetimes(time_zone=time_zone).map(
                        lambda d: d.py_datetime()
                    ),
                ),
                min_size=1,
            )
        )
        with assume_does_not_raise(
            ComputeError,  # unable to parse time zone
            ValueError,  # failed to parse timezone
        ):
            df = dataclass_to_dataframe(objs, localns=locals())
        check_polars_dataframe(
            df, height=len(objs), schema_list={"x": zoned_datetime(time_zone=time_zone)}
        )

    def test_error_empty(self) -> None:
        with raises(
            _DataClassToDataFrameEmptyError,
            match="At least 1 dataclass must be given; got 0",
        ):
            _ = dataclass_to_dataframe([])

    def test_error_non_unique(self) -> None:
        @dataclass(kw_only=True, slots=True)
        class Example1:
            x: int = 0

        @dataclass(kw_only=True, slots=True)
        class Example2:
            x: int = 0

        with raises(
            _DataClassToDataFrameNonUniqueError,
            match="Iterable .* must contain exactly 1 class; got .*, .* and perhaps more",
        ):
            _ = dataclass_to_dataframe([Example1(), Example2()])


class TestDataClassToSchema:
    def test_basic(self) -> None:
        today = get_today().py_date()

        @dataclass(kw_only=True, slots=True)
        class Example:
            bool_field: bool = False
            int_field: int = 0
            float_field: float = 0.0
            str_field: str = ""
            date_field: dt.date = today

        obj = Example()
        result = dataclass_to_schema(obj)
        expected = {
            "bool_field": Boolean,
            "int_field": Int64,
            "float_field": Float64,
            "str_field": String,
            "date_field": Date,
        }
        assert result == expected

    def test_basic_nullable(self) -> None:
        @dataclass(kw_only=True, slots=True)
        class Example:
            x: int | None = None

        obj = Example()
        result = dataclass_to_schema(obj)
        expected = {"x": Int64}
        assert result == expected

    def test_date_or_datetime_as_date(self) -> None:
        today = get_today().py_date()

        @dataclass(kw_only=True, slots=True)
        class Example:
            x: dt.date | dt.datetime = today

        obj = Example()
        result = dataclass_to_schema(obj)
        expected = {"x": Date}
        assert result == expected

    def test_date_or_datetime_as_local_datetime(self) -> None:
        now = get_now().to_plain().py_datetime()

        @dataclass(kw_only=True, slots=True)
        class Example:
            x: dt.date | dt.datetime = now

        obj = Example()
        result = dataclass_to_schema(obj)
        expected = {"x": Datetime()}
        assert result == expected

    @given(time_zone=timezones())
    def test_date_or_datetime_as_zoned_datetime(self, *, time_zone: ZoneInfo) -> None:
        with assume_does_not_raise(TimeZoneNotFoundError):
            now = get_now(time_zone=time_zone).py_datetime()

        @dataclass(kw_only=True, slots=True)
        class Example:
            x: dt.date | dt.datetime = now

        obj = Example()
        result = dataclass_to_schema(obj)
        expected = {"x": zoned_datetime(time_zone=time_zone)}
        assert result == expected

    def test_enum(self) -> None:
        class Truth(enum.Enum):
            true = auto()
            false = auto()

        @dataclass(kw_only=True, slots=True)
        class Example:
            x: Truth = Truth.true

        obj = Example()
        result = dataclass_to_schema(obj, localns=locals())
        expected = {"x": pl.Enum(["true", "false"])}
        assert result == expected

    def test_literal(self) -> None:
        @dataclass(kw_only=True, slots=True)
        class Example:
            x: Literal["true", "false"] = "true"

        obj = Example()
        result = dataclass_to_schema(obj)
        expected = {"x": pl.Enum(["true", "false"])}
        assert result == expected

    def test_local_datetime(self) -> None:
        now = get_now().to_plain().py_datetime()

        @dataclass(kw_only=True, slots=True)
        class Example:
            x: dt.datetime = now

        obj = Example()
        result = dataclass_to_schema(obj)
        expected = {"x": Datetime()}
        assert result == expected

    def test_nested_once(self) -> None:
        @dataclass(kw_only=True, slots=True)
        class Inner:
            x: int = 0

        @dataclass(kw_only=True, slots=True)
        class Outer:
            inner: Inner = field(default_factory=Inner)

        obj = Outer()
        result = dataclass_to_schema(obj, localns=locals())
        expected = {"inner": struct_dtype(x=Int64)}
        assert result == expected

    def test_nested_twice(self) -> None:
        @dataclass(kw_only=True, slots=True)
        class Inner:
            x: int = 0

        @dataclass(kw_only=True, slots=True)
        class Middle:
            inner: Inner = field(default_factory=Inner)

        @dataclass(kw_only=True, slots=True)
        class Outer:
            middle: Middle = field(default_factory=Middle)

        obj = Outer()
        result = dataclass_to_schema(obj, localns=locals())
        expected = {"middle": struct_dtype(inner=struct_dtype(x=Int64))}
        assert result == expected

    def test_nested_inner_list(self) -> None:
        @dataclass(kw_only=True, slots=True)
        class Inner:
            x: list[int] = field(default_factory=list)

        @dataclass(kw_only=True, slots=True)
        class Outer:
            inner: Inner = field(default_factory=Inner)

        obj = Outer()
        result = dataclass_to_schema(obj, localns=locals())
        expected = {"inner": Struct({"x": List(Int64)})}
        assert result == expected

    def test_nested_outer_list(self) -> None:
        @dataclass(kw_only=True, slots=True)
        class Inner:
            x: int = 0

        @dataclass(kw_only=True, slots=True)
        class Outer:
            inner: list[Inner] = field(default_factory=list)

        obj = Outer()
        result = dataclass_to_schema(obj, localns=locals())
        expected = {"inner": List(Struct({"x": Int64}))}
        assert result == expected

    def test_path(self) -> None:
        @dataclass(kw_only=True, slots=True)
        class Example:
            x: Path = PWD

        _ = Path  # add to locals
        obj = Example()
        result = dataclass_to_schema(obj, localns=locals())
        expected = {"x": Object}
        assert result == expected

    def test_uuid(self) -> None:
        @dataclass(kw_only=True, slots=True)
        class Example:
            x: UUID = field(default_factory=uuid4)

        _ = UUID  # add to locals
        obj = Example()
        result = dataclass_to_schema(obj, localns=locals())
        expected = {"x": Object}
        assert result == expected

    @given(time_zone=timezones())
    def test_zoned_datetime(self, *, time_zone: ZoneInfo) -> None:
        with assume_does_not_raise(TimeZoneNotFoundError):
            now = get_now(time_zone=time_zone).py_datetime()

        @dataclass(kw_only=True, slots=True)
        class Example:
            x: dt.datetime = now

        obj = Example()
        result = dataclass_to_schema(obj)
        expected = {"x": zoned_datetime(time_zone=time_zone)}
        assert result == expected

    @given(start=timezones(), end=timezones())
    def test_zoned_datetime_nested(self, *, start: ZoneInfo, end: ZoneInfo) -> None:
        with assume_does_not_raise(TimeZoneNotFoundError):
            now_start = get_now(time_zone=start).py_datetime()
            now_end = get_now(time_zone=end).py_datetime()

        @dataclass(kw_only=True, slots=True)
        class Inner:
            start: dt.datetime = now_start
            end: dt.datetime = now_end

        @dataclass(kw_only=True, slots=True)
        class Outer:
            inner: Inner = field(default_factory=Inner)

        obj = Outer()
        result = dataclass_to_schema(obj, localns=locals())
        expected = {
            "inner": Struct({
                "start": zoned_datetime(time_zone=start),
                "end": zoned_datetime(time_zone=end),
            })
        }
        assert result == expected

    def test_containers(self) -> None:
        @dataclass(kw_only=True, slots=True)
        class Example:
            frozenset_field: frozenset[int] = field(default_factory=frozenset)
            list_field: list[int] = field(default_factory=list)
            set_field: set[int] = field(default_factory=set)

        obj = Example()
        result = dataclass_to_schema(obj)
        expected = {
            "frozenset_field": List(Int64),
            "list_field": List(Int64),
            "set_field": List(Int64),
        }
        assert result == expected

    def test_error(self) -> None:
        @dataclass(kw_only=True, slots=True)
        class Example:
            x: Sentinel = sentinel

        obj = Example()
        with raises(NotImplementedError):
            _ = dataclass_to_schema(obj)


class TestDatetimeDTypes:
    @given(
        case=sampled_from([
            (HongKong, DatetimeHongKong),
            (Tokyo, DatetimeTokyo),
            (USCentral, DatetimeUSCentral),
            (USEastern, DatetimeUSEastern),
            (UTC, DatetimeUTC),
        ])
    )
    def test_main(self, *, case: tuple[ZoneInfo, Datetime]) -> None:
        time_zone, dtype = case
        name = get_time_zone_name(time_zone)
        expected = dtype.time_zone
        assert name == expected


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


class TestEnsureDataType:
    @given(dtype=sampled_from([Boolean, Boolean()]))
    def test_main(self, *, dtype: MaybeType[Boolean]) -> None:
        result = ensure_data_type(dtype)
        assert isinstance(result, DataType)
        assert isinstance(result, Boolean)


class TestEnsureExprOrSeries:
    @given(column=sampled_from(["column", col("column"), int_range(end=10)]))
    def test_main(self, *, column: IntoExprColumn) -> None:
        result = ensure_expr_or_series(column)
        assert isinstance(result, Expr | Series)


class TestEnsureExprOrSeriesMany:
    @given(column=sampled_from(["column", col("column"), int_range(end=10)]))
    def test_main(self, *, column: IntoExprColumn) -> None:
        result = ensure_expr_or_series_many(column, column=column)
        assert len(result) == 2
        for r in result:
            assert isinstance(r, Expr | Series)


class TestFiniteEWMMean:
    alpha_0_75_values: ClassVar[list[float]] = [
        -8.269850726503885,
        -8.067462681625972,
        3.233134329593507,
    ]
    alpha_0_99_values: ClassVar[list[float]] = [
        -8.970001998706891,
        -8.009700019987068,
        6.849902999800129,
    ]

    @given(
        case=sampled_from([
            [
                0.75,
                0.9,
                alpha_0_75_values,
                [-8.28235294117647, -8.070588235294117, 3.2705882352941176],
            ],
            [
                0.75,
                0.9999,
                alpha_0_75_values,
                [-8.269864158112174, -8.06746317849418, 3.2331284833087284],
            ],
            [
                0.99,
                0.9,
                alpha_0_99_values,
                [-8.970002970002971, -8.00970200970201, 6.849915849915851],
            ],
            [
                0.99,
                0.9999,
                alpha_0_99_values,
                [-8.970002000096999, -8.00970002000097, 6.84990300128499],
            ],
        ])
    )
    def test_main(self, *, case: tuple[float, float, list[float], list[float]]) -> None:
        alpha, min_weight, exp_base, exp_result = case
        state = get_state(seed=0)
        series = Series(values=[state.randint(-10, 10) for _ in range(100)])
        base = series.ewm_mean(alpha=alpha)
        exp_base_sr = Series(values=exp_base, dtype=Float64)
        assert_series_equal(base[-3:], exp_base_sr, check_names=False)
        result = finite_ewm_mean(series, alpha=alpha, min_weight=min_weight)
        exp_result_sr = Series(values=exp_result, dtype=Float64)
        assert_series_equal(result[-3:], exp_result_sr, check_names=False)

    def test_expr(self) -> None:
        expr = finite_ewm_mean(int_range(end=10), alpha=0.5)
        assert isinstance(expr, Expr)

    def test_error(self) -> None:
        with raises(
            FiniteEWMMeanError,
            match=r"Min weight must be at least 0 and less than 1; got 1\.0",
        ):
            _ = finite_ewm_mean(int_range(end=10), alpha=0.5, min_weight=1.0)


class TestFiniteEWMWeights:
    @given(alpha=floats(0.0001, 0.9999), min_weight=floats(0.0, 0.9999))
    def test_main(self, *, alpha: float, min_weight: float) -> None:
        weights = _finite_ewm_weights(alpha=alpha, min_weight=min_weight, raw=True)
        total = sum(weights)
        assert total >= min_weight

    def test_error(self) -> None:
        with raises(
            _FiniteEWMWeightsError,
            match=r"Min weight must be at least 0 and less than 1; got 1\.0",
        ):
            _ = _finite_ewm_weights(min_weight=1.0)


class TestFloorDateTime:
    start: ClassVar[dt.datetime] = dt.datetime(2000, 1, 1, 0, 0, tzinfo=UTC)
    end: ClassVar[dt.datetime] = dt.datetime(2000, 1, 1, 0, 3, tzinfo=UTC)
    expected: ClassVar[Series] = Series(
        values=[
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


class TestGetDataTypeOrSeriesTimeZone:
    @given(
        time_zone=sampled_from([HongKong, UTC]), case=sampled_from(["dtype", "series"])
    )
    def test_main(
        self, *, time_zone: ZoneInfo, case: Literal["dtype", "series"]
    ) -> None:
        dtype = zoned_datetime(time_zone=time_zone)
        match case:
            case "dtype":
                dtype_or_series = dtype
            case "series":
                dtype_or_series = Series(dtype=dtype)
        result = get_data_type_or_series_time_zone(dtype_or_series)
        assert result is time_zone

    def test_error_not_datetime(self) -> None:
        with raises(
            _GetDataTypeOrSeriesTimeZoneNotDateTimeError,
            match="Data type must be Datetime; got Boolean",
        ):
            _ = get_data_type_or_series_time_zone(Boolean)

    def test_error_not_zoned(self) -> None:
        with raises(
            _GetDataTypeOrSeriesTimeZoneNotZonedError,
            match="Data type must be zoned; got .*",
        ):
            _ = get_data_type_or_series_time_zone(Datetime)


class TestGetExprName:
    @given(n=hypothesis.strategies.integers(0, 10), name=text_ascii())
    def test_series(self, *, n: int, name: str) -> None:
        sr = int_range(n, eager=True)
        expr = lit(None, dtype=Boolean).alias(name)
        result = get_expr_name(sr, expr)
        assert result == name

    @given(n=hypothesis.strategies.integers(0, 10), name=text_ascii())
    def test_df(self, *, n: int, name: str) -> None:
        df = int_range(n, eager=True).to_frame()
        expr = lit(None, dtype=Boolean).alias(name)
        result = get_expr_name(df, expr)
        assert result == name


class TestGetFrequencySpectrum:
    def test_main(self) -> None:
        n = 1000
        x = linspace(0, 2 * pi, n)
        noise = DEFAULT_RNG.normal(scale=0.25, size=n)
        y = Series(x + noise)
        y2 = adjust_frequencies(y, filters=lambda f: np.abs(f) <= 0.02)
        result = get_frequency_spectrum(y2)
        check_polars_dataframe(
            result, height=n, schema_list={"frequency": Float64, "amplitude": Float64}
        )
        assert allclose(result.filter(col("frequency").abs() > 0.02)["amplitude"], 0.0)


class TestGetSeriesNumberOfDecimals:
    @given(data=data(), n=hypothesis.strategies.integers(1, 10), nullable=booleans())
    def test_main(self, *, data: DataObject, n: int, nullable: bool) -> None:
        strategy = int64s() | none() if nullable else int64s()
        ints_or_none = data.draw(lists(strategy, min_size=1, max_size=10))
        values = [None if i is None else i / 10**n for i in ints_or_none]
        series = Series(values=values, dtype=Float64)
        result = get_series_number_of_decimals(series, nullable=nullable)
        if not nullable:
            assert result is not None
            expected = max(number_of_decimals(v) for v in values if v is not None)
            assert result == expected

    def test_error_not_float(self) -> None:
        with raises(
            _GetSeriesNumberOfDecimalsNotFloatError,
            match="Data type must be Float64; got Boolean",
        ):
            _ = get_series_number_of_decimals(Series(dtype=Boolean))

    def test_error_not_zoned(self) -> None:
        with raises(
            _GetSeriesNumberOfDecimalsAllNullError,
            match="Series must not be all-null; got .*",
        ):
            _ = get_series_number_of_decimals(Series(dtype=Float64))


class TestInsertBeforeOrAfter:
    df: ClassVar[DataFrame] = DataFrame(schema={"a": Int64, "b": Int64, "c": Int64})

    @given(
        case=sampled_from([
            ("a", ["a", "new", "b", "c"]),
            ("b", ["a", "b", "new", "c"]),
            ("c", ["a", "b", "c", "new"]),
        ])
    )
    def test_after(self, *, case: tuple[str, list[str]]) -> None:
        column, expected = case
        for _ in range(2):  # guard against in-place
            result = insert_after(self.df, column, lit(None).alias("new"))
            assert result.columns == expected

    @given(
        case=sampled_from([
            ("a", ["new", "a", "b", "c"]),
            ("b", ["a", "new", "b", "c"]),
            ("c", ["a", "b", "new", "c"]),
        ])
    )
    def test_before(self, *, case: tuple[str, list[str]]) -> None:
        column, expected = case
        for _ in range(2):  # guard against in-place
            result = insert_before(self.df, column, lit(None).alias("new"))
            assert result.columns == expected

    @given(
        case=sampled_from([
            (insert_before, InsertBeforeError),
            (insert_after, InsertAfterError),
        ])
    )
    def test_error(
        self,
        *,
        case: tuple[Callable[[DataFrame, str, IntoExprColumn]], type[Exception]],
    ) -> None:
        func, error = case
        with raises(error, match="DataFrame must have column 'missing'; got .*"):
            _ = func(self.df, "missing", lit(None).alias("new"))


class TestInsertBetween:
    df: ClassVar[DataFrame] = DataFrame(schema={"a": Int64, "b": Int64, "c": Int64})

    @given(
        case=sampled_from([
            ("a", "b", ["a", "new", "b", "c"]),
            ("b", "c", ["a", "b", "new", "c"]),
        ])
    )
    def test_main(self, *, case: tuple[str, str, list[str]]) -> None:
        left, right, expected = case
        for _ in range(2):  # guard against in-place
            result = insert_between(self.df, left, right, lit(None).alias("new"))
            assert result.columns == expected

    def test_error_missing(self) -> None:
        with raises(
            _InsertBetweenMissingColumnsError,
            match="DataFrame must have columns 'x' and 'y'; got .*",
        ):
            _ = insert_between(self.df, "x", "y", lit(None).alias("new"))

    def test_error_non_consecutive(self) -> None:
        with raises(
            _InsertBetweenNonConsecutiveError,
            match="DataFrame columns 'a' and 'c' must be consecutive; got indices 0 and 2",
        ):
            _ = insert_between(self.df, "a", "c", lit(None).alias("new"))


class TestIntegers:
    @given(
        length=hypothesis.strategies.integers(0, 10),
        high=hypothesis.strategies.integers(1, 10),
    )
    def test_int(self, *, length: int, high: int) -> None:
        series = utilities.polars.integers(length, high)
        self._assert(series, length, high)

    @given(
        length=hypothesis.strategies.integers(0, 10),
        high=hypothesis.strategies.integers(1, 10),
    )
    def test_series(self, *, length: int, high: int) -> None:
        orig = int_range(end=length, eager=True)
        series = utilities.polars.integers(orig, high)
        self._assert(series, length, high)

    @given(
        length=hypothesis.strategies.integers(0, 10),
        high=hypothesis.strategies.integers(1, 10),
    )
    def test_dataframe(self, *, length: int, high: int) -> None:
        df = int_range(end=length, eager=True).to_frame()
        series = utilities.polars.integers(df, high)
        self._assert(series, length, high)

    def _assert(self, series: Series, length: int, high: int, /) -> None:
        assert series.dtype == Int64
        assert series.len() == length
        assert series.is_between(0, high, closed="left").all()


class TestIsNearEvent:
    df: ClassVar[DataFrame] = DataFrame(
        data=[
            (False, False),
            (False, False),
            (True, False),
            (True, False),
            (False, False),
            (False, False),
            (False, False),
            (False, False),
            (False, False),
            (False, True),
        ],
        schema={"x": Boolean, "y": Boolean},
        orient="row",
    )

    def test_no_exprs(self) -> None:
        result = self.df.with_columns(is_near_event().alias("z"))["z"]
        expected = Series(
            name="z", values=list(repeat(object=False, times=10)), dtype=Boolean
        )
        assert_series_equal(result, expected)

    def test_x(self) -> None:
        result = self.df.with_columns(is_near_event("x").alias("z"))["z"]
        expected = Series(
            name="z",
            values=[False, False, True, True, False, False, False, False, False, False],
            dtype=Boolean,
        )
        assert_series_equal(result, expected)

    def test_y(self) -> None:
        result = self.df.with_columns(is_near_event("y").alias("z"))["z"]
        expected = Series(
            name="z",
            values=[
                False,
                False,
                False,
                False,
                False,
                False,
                False,
                False,
                False,
                True,
            ],
            dtype=Boolean,
        )
        assert_series_equal(result, expected)

    def test_x_before(self) -> None:
        result = self.df.with_columns(is_near_event("x", before=1).alias("z"))["z"]
        expected = Series(
            name="z",
            values=[False, True, True, True, False, False, False, False, False, False],
            dtype=Boolean,
        )
        assert_series_equal(result, expected)

    def test_x_after(self) -> None:
        result = self.df.with_columns(is_near_event("x", after=1).alias("z"))["z"]
        expected = Series(
            name="z",
            values=[False, False, True, True, True, False, False, False, False, False],
            dtype=Boolean,
        )
        assert_series_equal(result, expected)

    def test_x_or_y(self) -> None:
        result = self.df.with_columns(is_near_event("x", "y").alias("z"))["z"]
        expected = Series(
            name="z",
            values=[False, False, True, True, False, False, False, False, False, True],
            dtype=Boolean,
        )
        assert_series_equal(result, expected)

    @given(before=hypothesis.strategies.integers(max_value=-1))
    def test_error_before(self, *, before: int) -> None:
        with raises(
            _IsNearEventBeforeError, match=r"'Before' must be non-negative; got \-\d+"
        ):
            _ = is_near_event(before=before)

    @given(after=hypothesis.strategies.integers(max_value=-1))
    def test_error_after(self, *, after: int) -> None:
        with raises(
            _IsNearEventAfterError, match=r"'After' must be non-negative; got \-\d+"
        ):
            _ = is_near_event(after=after)


class TestIsNullAndIsNotNullStructSeries:
    @given(
        case=sampled_from([
            (is_null_struct_series, [True, False, False, False]),
            (is_not_null_struct_series, [False, True, True, True]),
        ])
    )
    def test_main(self, *, case: tuple[Callable[[Series], Series], list[bool]]) -> None:
        func, exp_values = case
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
        expected = Series(values=exp_values, dtype=Boolean)
        assert_series_equal(result, expected)

    @given(
        case=sampled_from([
            (is_null_struct_series, [False, False, False, True]),
            (is_not_null_struct_series, [True, True, True, False]),
        ])
    )
    def test_nested(
        self, *, case: tuple[Callable[[Series], Series], list[bool]]
    ) -> None:
        func, exp_values = case
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
        expected = Series(values=exp_values, dtype=Boolean)
        assert_series_equal(result, expected)

    @given(
        case=sampled_from([
            (is_null_struct_series, IsNullStructSeriesError),
            (is_not_null_struct_series, IsNotNullStructSeriesError),
        ])
    )
    def test_error(
        self, *, case: tuple[Callable[[Series], Series], type[Exception]]
    ) -> None:
        func, error = case
        series = Series(name="series", values=[1, 2, 3, None], dtype=Int64)
        with raises(error, match="Series must have Struct-dtype; got Int64"):
            _ = func(series)


class TestJoin:
    def test_main(self) -> None:
        df1 = DataFrame(data=[{"a": 1, "b": 2}], schema={"a": Int64, "b": Int64})
        df2 = DataFrame(data=[{"a": 1, "c": 3}], schema={"a": Int64, "c": Int64})
        result = join(df1, df2, on="a")
        expected = DataFrame(
            data=[{"a": 1, "b": 2, "c": 3}], schema={"a": Int64, "b": Int64, "c": Int64}
        )
        assert_frame_equal(result, expected)


class TestJoinIntoPeriods:
    dtype: ClassVar[Struct] = struct_dtype(start=DatetimeUTC, end=DatetimeUTC)

    @mark.parametrize("on", [param("datetime"), param(None)])
    def test_main(self, *, on: str | None) -> None:
        df1, df2, expected = self._prepare_main()
        result = join_into_periods(df1, df2, on=on)
        assert_frame_equal(result, expected)

    def test_left_on_and_right_on(self) -> None:
        df1, df2, expected = self._prepare_main(right="period", joined_second="period")
        result = join_into_periods(df1, df2, left_on="datetime", right_on="period")
        assert_frame_equal(result, expected)

    def test_overlapping_bar(self) -> None:
        times = [(dt.time(), dt.time(1, 30))]
        df1 = self._lift_df(times)
        periods = [(dt.time(1), dt.time(2)), (dt.time(2), dt.time(3))]
        df2 = self._lift_df(periods)
        result = join_into_periods(df1, df2, on="datetime")
        df3 = self._lift_df([None], column="datetime_right")
        expected = concat([df1, df3], how="horizontal")
        assert_frame_equal(result, expected)

    def _prepare_main(
        self,
        *,
        left: str = "datetime",
        right: str = "datetime",
        joined_second: str = "datetime_right",
    ) -> tuple[DataFrame, DataFrame, DataFrame]:
        times = [
            (dt.time(), dt.time(0, 30)),
            (dt.time(0, 30), dt.time(1)),
            (dt.time(1), dt.time(1, 30)),
            (dt.time(1, 30), dt.time(2)),
            (dt.time(2), dt.time(2, 30)),
            (dt.time(2, 30), dt.time(3)),
            (dt.time(3), dt.time(3, 30)),
            (dt.time(3, 30), dt.time(4)),
            (dt.time(4), dt.time(4, 30)),
            (dt.time(4, 30), dt.time(5)),
        ]
        df1 = self._lift_df(times, column=left)
        periods = [
            (dt.time(1), dt.time(2)),
            (dt.time(2), dt.time(3)),
            (dt.time(3), dt.time(4)),
        ]
        df2 = self._lift_df(periods, column=right)
        joined = [
            None,
            None,
            (dt.time(1), dt.time(2)),
            (dt.time(1), dt.time(2)),
            (dt.time(2), dt.time(3)),
            (dt.time(2), dt.time(3)),
            (dt.time(3), dt.time(4)),
            (dt.time(3), dt.time(4)),
            None,
            None,
        ]
        df3 = self._lift_df(joined, column=joined_second)
        expected = concat([df1, df3], how="horizontal")
        return df1, df2, expected

    def test_error_arguments(self) -> None:
        with raises(
            _JoinIntoPeriodsArgumentsError,
            match="Either 'on' must be given or 'left_on' and 'right_on' must be given; got None, 'datetime' and None",
        ):
            _ = join_into_periods(DataFrame(), DataFrame(), left_on="datetime")

    def test_error_periods(self) -> None:
        times = [(dt.time(1), dt.time())]
        df = self._lift_df(times)
        with raises(
            _JoinIntoPeriodsPeriodError,
            match="Left DataFrame column 'datetime' must contain valid periods",
        ):
            _ = join_into_periods(df, DataFrame())

    def test_error_left_start_sorted(self) -> None:
        times = [(dt.time(1), dt.time(2)), (dt.time(), dt.time(1))]
        df = self._lift_df(times)
        with raises(
            _JoinIntoPeriodsSortedError,
            match="Left DataFrame column 'datetime/start' must be sorted",
        ):
            _ = join_into_periods(df, df)

    def test_error_end_sorted(self) -> None:
        times = [(dt.time(), dt.time(3)), (dt.time(1), dt.time(2))]
        df = self._lift_df(times)
        with raises(
            _JoinIntoPeriodsSortedError,
            match="Left DataFrame column 'datetime/end' must be sorted",
        ):
            _ = join_into_periods(df, df)

    def test_error_overlapping(self) -> None:
        times = [(dt.time(), dt.time(2)), (dt.time(1), dt.time(3))]
        df = self._lift_df(times)
        with raises(
            _JoinIntoPeriodsOverlappingError,
            match="Left DataFrame column 'datetime' must not contain overlaps",
        ):
            _ = join_into_periods(df, DataFrame())

    def _lift_df(
        self,
        times: Iterable[tuple[dt.time, dt.time] | None],
        /,
        *,
        column: str = "datetime",
    ) -> DataFrame:
        return DataFrame(
            data=[self._lift_row(t, column=column) for t in times],
            schema={column: self.dtype},
            orient="row",
        )

    def _lift_row(
        self, times: tuple[dt.time, dt.time] | None, /, *, column: str = "datetime"
    ) -> StrMapping | None:
        if times is None:
            return None
        start, end = times
        return {column: {"start": self._lift_time(start), "end": self._lift_time(end)}}

    def _lift_time(self, time: dt.time, /) -> dt.datetime:
        return dt.datetime.combine(get_today().py_date(), time, tzinfo=UTC)


class TestMapOverColumns:
    def test_series(self) -> None:
        series = Series(values=[1, 2, 3], dtype=Int64)
        result = map_over_columns(lambda x: 2 * x, series)
        expected = 2 * series
        assert_series_equal(result, expected)

    def test_series_nested(self) -> None:
        dtype = struct_dtype(outer=Int64, inner=struct_dtype(value=Int64))
        series = Series(
            values=[
                {"outer": 1, "inner": {"value": 2}},
                {"outer": 3, "inner": {"value": 4}},
                {"outer": 5, "inner": {"value": 6}},
            ],
            dtype=dtype,
        )
        result = map_over_columns(lambda x: 2 * x, series)
        expected = Series(
            values=[
                {"outer": 2, "inner": {"value": 4}},
                {"outer": 6, "inner": {"value": 8}},
                {"outer": 10, "inner": {"value": 12}},
            ],
            dtype=dtype,
        )
        assert_series_equal(result, expected)

    def test_dataframe(self) -> None:
        df = DataFrame(data=[(1,), (2,), (3,)], schema={"value": Int64}, orient="row")
        result = map_over_columns(lambda x: 2 * x, df)
        expected = 2 * df
        assert_frame_equal(result, expected)

    def test_dataframe_nested(self) -> None:
        schema = {"outer": Int64, "inner": struct_dtype(value=Int64)}
        df = DataFrame(
            data=[
                {"outer": 1, "inner": {"value": 2}},
                {"outer": 3, "inner": {"value": 4}},
                {"outer": 5, "inner": {"value": 6}},
            ],
            schema=schema,
            orient="row",
        )
        result = map_over_columns(lambda x: 2 * x, df)
        expected = DataFrame(
            data=[
                {"outer": 2, "inner": {"value": 4}},
                {"outer": 6, "inner": {"value": 8}},
                {"outer": 10, "inner": {"value": 12}},
            ],
            schema=schema,
            orient="row",
        )
        assert_frame_equal(result, expected)

    def test_dataframe_nested_twice(self) -> None:
        schema = {
            "outer": Int64,
            "middle": struct_dtype(mvalue=Int64, inner=struct_dtype(ivalue=Int64)),
        }
        df = DataFrame(
            data=[
                {"outer": 1, "middle": {"mvalue": 2, "inner": {"ivalue": 3}}},
                {"outer": 4, "middle": {"mvalue": 5, "inner": {"ivalue": 6}}},
                {"outer": 7, "middle": {"mvalue": 8, "inner": {"ivalue": 9}}},
            ],
            schema=schema,
            orient="row",
        )
        result = map_over_columns(lambda x: 2 * x, df)
        expected = DataFrame(
            data=[
                {"outer": 2, "middle": {"mvalue": 4, "inner": {"ivalue": 6}}},
                {"outer": 8, "middle": {"mvalue": 10, "inner": {"ivalue": 12}}},
                {"outer": 14, "middle": {"mvalue": 16, "inner": {"ivalue": 18}}},
            ],
            schema=schema,
            orient="row",
        )
        assert_frame_equal(result, expected)


class TestNanSumAgg:
    @given(
        case=sampled_from([
            ([None], None),
            ([None, None], None),
            ([0], 0),
            ([0, None], 0),
            ([0, None, None], 0),
            ([1, 2], 3),
            ([1, 2, None], 3),
            ([1, 2, None, None], 3),
        ]),
        dtype=sampled_from([Int64, Float64]),
        mode=sampled_from(["str", "column"]),
    )
    def test_main(
        self,
        *,
        case: tuple[list[Any], int | None],
        dtype: DataTypeClass,
        mode: Literal["str", "column"],
    ) -> None:
        values, expected = case
        df = DataFrame(data=values, schema={"value": dtype}).with_columns(id=lit("id"))
        match mode:
            case "str":
                agg = "value"
            case "column":
                agg = col("value")
        result = df.group_by("id").agg(nan_sum_agg(agg))
        assert result["value"].item() == expected


class TestNanSumCols:
    @given(
        case=sampled_from([(None, None, None), (None, 0, 0), (0, None, 0), (1, 2, 3)]),
        x_kind=sampled_from(["str", "column"]),
        y_kind=sampled_from(["str", "column"]),
    )
    def test_main(
        self,
        *,
        case: tuple[int | None, int | None, int | None],
        x_kind: Literal["str", "column"],
        y_kind: Literal["str", "column"],
    ) -> None:
        x, y, expected = case
        x_use = "x" if x_kind == "str" else col("x")
        y_use = "y" if y_kind == "str" else col("y")
        df = DataFrame(
            data=[(x, y)], schema={"x": Int64, "y": Int64}, orient="row"
        ).with_columns(z=nan_sum_cols(x_use, y_use))
        assert df["z"].item() == expected


class TestNormal:
    @given(length=hypothesis.strategies.integers(0, 10))
    def test_int(self, *, length: int) -> None:
        series = normal(length)
        self._assert(series, length)

    @given(length=hypothesis.strategies.integers(0, 10))
    def test_series(self, *, length: int) -> None:
        orig = int_range(end=length, eager=True)
        series = normal(orig)
        self._assert(series, length)

    @given(length=hypothesis.strategies.integers(0, 10))
    def test_dataframe(self, *, length: int) -> None:
        df = int_range(end=length, eager=True).to_frame()
        series = normal(df)
        self._assert(series, length)

    def _assert(self, series: Series, length: int, /) -> None:
        assert series.dtype == Float64
        assert series.len() == length
        assert series.is_finite().all()


class TestOrderOfMagnitude:
    @given(
        sign=sampled_from([1, -1]),
        case=sampled_from([
            (0.25, -0.60206, -1),
            (0.5, -0.30103, 0),
            (0.75, -0.1249387, 0),
            (1.0, 0.0, 0),
            (5.0, 0.69897, 1),
            (10.0, 1.0, 1),
            (50.0, 1.69897, 2),
            (100.0, 2.0, 2),
        ]),
    )
    def test_main(self, *, sign: int, case: tuple[float, float, int]) -> None:
        x, exp_float, exp_int = case
        x_use = Series(values=[sign * x])
        res_float = order_of_magnitude(x_use)
        assert res_float.dtype == Float64
        assert_series_equal(res_float, Series([exp_float]))
        res_int = order_of_magnitude(x_use, round_=True)
        assert res_int.dtype == Int64
        assert_series_equal(res_int, Series([exp_int]))
        assert (res_int == exp_int).all()


class TestReifyExprs:
    @given(length=hypothesis.strategies.integers(0, 10), name=text_ascii())
    def test_one_expr(self, *, length: int, name: str) -> None:
        expr = int_range(end=length).alias(name)
        result = reify_exprs(expr)
        assert isinstance(result, Expr)
        result2 = (
            int_range(end=length, eager=True)
            .alias(f"_{name}")
            .to_frame()
            .with_columns(result)[name]
        )
        expected = int_range(end=length, eager=True).alias(name)
        assert_series_equal(result2, expected)

    @given(length=hypothesis.strategies.integers(0, 10), name=text_ascii())
    def test_one_series(self, *, length: int, name: str) -> None:
        series = int_range(end=length, eager=True).alias(name)
        result = reify_exprs(series)
        assert isinstance(result, Series)
        assert_series_equal(result, series)

    @given(
        length=hypothesis.strategies.integers(0, 10),
        names=pairs(text_ascii(), unique=True),
    )
    def test_two_exprs(self, *, length: int, names: tuple[str, str]) -> None:
        name1, name2 = names
        expr1 = int_range(end=length).alias(name1)
        expr2 = int_range(end=length).alias(name2)
        result = reify_exprs(expr1, expr2)
        assert isinstance(result, Expr)
        result2 = (
            int_range(end=length, eager=True)
            .alias(f"_{names}")
            .to_frame()
            .with_columns(result)[name1]
        )
        assert result2.name == name1
        assert result2.dtype == Struct(dict.fromkeys(names, Int64))

    @given(
        length=hypothesis.strategies.integers(0, 10),
        names=pairs(text_ascii(), unique=True),
    )
    def test_one_expr_and_one_series(
        self, *, length: int, names: tuple[str, str]
    ) -> None:
        name1, name2 = names
        expr = int_range(end=length).alias(name1)
        series = int_range(end=length, eager=True).alias(name2)
        result = reify_exprs(expr, series)
        assert isinstance(result, DataFrame)
        assert result.schema == dict.fromkeys(names, Int64)

    @given(
        length=hypothesis.strategies.integers(0, 10),
        names=pairs(text_ascii(), unique=True),
    )
    def test_two_series(self, *, length: int, names: tuple[str, str]) -> None:
        name1, name2 = names
        series1 = int_range(end=length, eager=True).alias(name1)
        series2 = int_range(end=length, eager=True).alias(name2)
        result = reify_exprs(series1, series2)
        assert isinstance(result, DataFrame)
        expected = concat_series(series1, series2)
        assert_frame_equal(result, expected)

    def test_error_empty(self) -> None:
        with raises(
            _ReifyExprsEmptyError, match="At least 1 Expression or Series must be given"
        ):
            _ = reify_exprs()

    @given(
        lengths=pairs(hypothesis.strategies.integers(0, 10), unique=True),
        names=pairs(text_ascii(), unique=True),
    )
    def test_error_non_unique(
        self, *, lengths: tuple[int, int], names: tuple[str, str]
    ) -> None:
        series1, series2 = [
            int_range(end=length, eager=True).alias(name)
            for length, name in zip(lengths, names, strict=True)
        ]
        with raises(
            _ReifyExprsSeriesNonUniqueError,
            match=r"Series must contain exactly one length; got \d+, \d+ and perhaps more",
        ):
            _ = reify_exprs(series1, series2)


class TestReplaceTimeZone:
    def test_datetime(self) -> None:
        now_utc = get_now().py_datetime()
        series = Series(values=[now_utc], dtype=DatetimeUTC)
        result = replace_time_zone(series, time_zone=None)
        expected = Series(values=[now_utc.replace(tzinfo=None)], dtype=Datetime)
        assert_series_equal(result, expected)

    def test_non_datetime(self) -> None:
        series = Series(name="series", values=[True], dtype=Boolean)
        result = replace_time_zone(series, time_zone=None)
        assert_series_equal(result, series)


class TestSerializeAndDeserializeDataFrame:
    cases: ClassVar[list[tuple[PolarsDataType, SearchStrategy[Any]]]] = [
        (Boolean, booleans()),
        (Boolean(), booleans()),
        (Date, hypothesis.strategies.dates()),
        (Date(), hypothesis.strategies.dates()),
        (Datetime(), py_datetimes(zoned=False)),
        (Datetime(time_zone=UTC.key), py_datetimes(zoned=True)),
        (Int64, int64s()),
        (Int64(), int64s()),
        (Float64, float64s()),
        (Float64(), float64s()),
        (String, text_ascii()),
        (String(), text_ascii()),
        (List(Int64), lists(int64s())),
        (Struct({"inner": Int64}), fixed_dictionaries({"inner": int64s()})),
    ]

    @given(data=data(), root=temp_paths(), name=text_ascii(), case=sampled_from(cases))
    def test_series(
        self,
        *,
        data: DataObject,
        root: Path,
        name: str,
        case: tuple[PolarsDataType, SearchStrategy[Any]],
    ) -> None:
        dtype, strategy = case
        values = data.draw(lists(strategy | none()))
        sr = Series(name=name, values=values, dtype=dtype)
        result1 = deserialize_series(serialize_series(sr))
        assert_series_equal(sr, result1)
        write_series(sr, file := root.joinpath("file.json"))
        result2 = read_series(file)
        assert_series_equal(sr, result2)

    @given(data=data(), root=temp_paths(), case=sampled_from(cases))
    def test_dataframe(
        self,
        *,
        data: DataObject,
        root: Path,
        case: tuple[PolarsDataType, SearchStrategy[Any]],
    ) -> None:
        dtype, strategy = case
        columns = data.draw(lists(text_ascii(min_size=1)))
        rows = data.draw(
            lists(fixed_dictionaries({c: strategy | none() for c in columns}))
        )
        schema = dict.fromkeys(columns, dtype)
        df = DataFrame(data=rows, schema=schema, orient="row")
        result1 = deserialize_dataframe(serialize_dataframe(df))
        assert_frame_equal(df, result1)
        write_dataframe(df, file := root.joinpath("file.json"))
        result2 = read_dataframe(file)
        assert_frame_equal(df, result2)

    @given(dtype=sampled_from([dtype for dtype, _ in cases]))
    def test_dtype(self, *, dtype: PolarsDataType) -> None:
        result = _reconstruct_dtype(_deconstruct_dtype(dtype))
        assert result == dtype

    @given(dtype=sampled_from([dtype for dtype, _ in cases]))
    def test_schema(self, *, dtype: PolarsDataType) -> None:
        schema = Schema({"column": dtype})
        result = _reconstruct_schema(_deconstruct_schema(schema))
        assert result == schema


class TestSetFirstRowAsColumns:
    def test_empty(self) -> None:
        df = DataFrame()
        with raises(
            SetFirstRowAsColumnsError,
            match="DataFrame must have at least 1 row; got .*",
        ):
            _ = set_first_row_as_columns(df)

    def test_one_row(self) -> None:
        df = DataFrame(data=["value"])
        check_polars_dataframe(df, height=1, schema_list={"column_0": String})
        result = set_first_row_as_columns(df)
        check_polars_dataframe(result, height=0, schema_list={"value": String})

    def test_multiple_rows(self) -> None:
        df = DataFrame(data=["foo", "bar", "baz"])
        check_polars_dataframe(df, height=3, schema_list={"column_0": String})
        result = set_first_row_as_columns(df)
        check_polars_dataframe(result, height=2, schema_list={"foo": String})


class TestStructDType:
    def test_main(self) -> None:
        result = struct_dtype(start=DatetimeUTC, end=DatetimeUTC)
        expected = Struct({"start": DatetimeUTC, "end": DatetimeUTC})
        assert result == expected


class TestStructFromDataClass:
    def test_simple(self) -> None:
        @dataclass(kw_only=True, slots=True)
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

        result = struct_from_dataclass(Example, globalns=globals())
        expected = Struct({
            "bool_": Boolean,
            "bool_maybe": Boolean,
            "date": Date,
            "date_maybe": Date,
            "float_": Float64,
            "float_maybe": Float64,
            "int_": Int64,
            "int_maybe": Int64,
            "str_": String,
            "str_maybe": String,
        })
        assert result == expected

    def test_datetime(self) -> None:
        @dataclass(kw_only=True, slots=True)
        class Example:
            field: dt.datetime

        result = struct_from_dataclass(Example, time_zone=UTC, globalns=globals())
        expected = Struct({"field": DatetimeUTC})
        assert result == expected

    def test_enum(self) -> None:
        class Truth(enum.Enum):
            true = auto()
            false = auto()

        @dataclass(kw_only=True, slots=True)
        class Example:
            field: Truth

        result = struct_from_dataclass(Example, localns=locals())
        expected = Struct({"field": String})
        assert result == expected

    def test_literal(self) -> None:
        LowOrHigh = Literal["low", "high"]  # noqa: N806

        @dataclass(kw_only=True, slots=True)
        class Example:
            field: LowOrHigh  # pyright: ignore[reportInvalidTypeForm]

        result = struct_from_dataclass(Example, localns=locals())
        expected = Struct({"field": String})
        assert result == expected

    def test_containers(self) -> None:
        @dataclass(kw_only=True, slots=True)
        class Example:
            frozenset_: frozenset[int]
            list_: list[int]
            set_: set[int]

        result = struct_from_dataclass(Example, time_zone=UTC)
        expected = Struct({
            "frozenset_": List(Int64),
            "list_": List(Int64),
            "set_": List(Int64),
        })
        assert result == expected

    def test_list_of_struct(self) -> None:
        @dataclass(kw_only=True, slots=True)
        class Inner:
            field: int

        @dataclass(kw_only=True, slots=True)
        class Outer:
            inner: list[Inner]

        result = struct_from_dataclass(Outer, localns=locals(), time_zone=UTC)
        expected = Struct({"inner": List(Struct({"field": Int64}))})
        assert result == expected

    def test_struct(self) -> None:
        @dataclass(kw_only=True, slots=True)
        class Inner:
            field: int

        @dataclass(kw_only=True, slots=True)
        class Outer:
            inner: Inner

        result = struct_from_dataclass(Outer, localns=locals(), time_zone=UTC)
        expected = Struct({"inner": Struct({"field": Int64})})
        assert result == expected

    def test_struct_of_list(self) -> None:
        @dataclass(kw_only=True, slots=True)
        class Inner:
            field: list[int]

        @dataclass(kw_only=True, slots=True)
        class Outer:
            inner: Inner

        result = struct_from_dataclass(Outer, localns=locals(), time_zone=UTC)
        expected = Struct({"inner": Struct({"field": List(Int64)})})
        assert result == expected

    def test_not_a_dataclass_error(self) -> None:
        with raises(
            StructFromDataClassError, match="Object must be a dataclass; got None"
        ):
            _ = struct_from_dataclass(cast("Any", None))

    def test_missing_time_zone_error(self) -> None:
        @dataclass(kw_only=True, slots=True)
        class Example:
            field: dt.datetime

        with raises(StructFromDataClassError, match="Time-zone must be given"):
            _ = struct_from_dataclass(Example, globalns=globals())

    def test_missing_type_error(self) -> None:
        @dataclass(kw_only=True, slots=True)
        class Example:
            field: None

        with raises(
            StructFromDataClassError, match="Unsupported type: <class 'NoneType'>"
        ):
            _ = struct_from_dataclass(Example)


class TestTryReifyExpr:
    # expr

    @given(length=hypothesis.strategies.integers(0, 10), name=text_ascii())
    def test_flat_expr(self, *, length: int, name: str) -> None:
        expr = int_range(end=length).alias(name)
        result = try_reify_expr(expr)
        assert isinstance(result, Expr)
        result2 = (
            int_range(end=length, eager=True)
            .alias(f"_{name}")
            .to_frame()
            .with_columns(result)[name]
        )
        expected = int_range(end=length, eager=True).alias(name)
        assert_series_equal(result2, expected)

    @given(length=hypothesis.strategies.integers(0, 10), names=pairs(text_ascii()))
    def test_flat_expr_and_expr(self, *, length: int, names: tuple[str, str]) -> None:
        name1, name2 = names
        expr1 = int_range(end=length).alias(name1)
        expr2 = int_range(end=length).alias(name2)
        result = try_reify_expr(expr1, expr2)
        assert isinstance(result, Expr)
        result2 = (
            int_range(end=length, eager=True)
            .alias(f"_{name1}")
            .to_frame()
            .with_columns(result)[name1]
        )
        expected = int_range(end=length, eager=True).alias(name1)
        assert_series_equal(result2, expected)

    @given(length=hypothesis.strategies.integers(0, 10), names=pairs(text_ascii()))
    def test_flat_expr_and_series(self, *, length: int, names: tuple[str, str]) -> None:
        name1, name2 = names
        expr = int_range(end=length).alias(name1)
        series = int_range(end=length, eager=True).alias(name2)
        result = try_reify_expr(expr, series)
        assert isinstance(result, Series)
        assert_series_equal(result, series.alias(name1))

    @given(length=hypothesis.strategies.integers(0, 10), name=text_ascii())
    def test_struct_expr(self, *, length: int, name: str) -> None:
        expr = struct(int_range(end=length).alias(name)).alias(name)
        result = try_reify_expr(expr)
        assert isinstance(result, Expr)
        result2 = (
            int_range(end=length, eager=True)
            .alias(f"_{name}")
            .to_frame()
            .with_columns(result)[name]
        )
        expected = (
            int_range(end=length, eager=True)
            .alias(name)
            .to_frame()
            .select(struct(name))[name]
        )
        assert_series_equal(result2, expected)

    @given(length=hypothesis.strategies.integers(0, 10), names=pairs(text_ascii()))
    def test_struct_expr_and_expr(self, *, length: int, names: tuple[str, str]) -> None:
        name1, name2 = names
        expr1 = struct(int_range(end=length).alias(name1)).alias(name1)
        expr2 = int_range(end=length).alias(name2)
        result = try_reify_expr(expr1, expr2)
        assert isinstance(result, Expr)
        result2 = (
            int_range(end=length, eager=True)
            .alias(f"_{name1}")
            .to_frame()
            .with_columns(result)[name1]
        )
        expected = (
            int_range(end=length, eager=True)
            .alias(name1)
            .to_frame()
            .select(struct(name1))[name1]
        )
        assert_series_equal(result2, expected)

    @given(length=hypothesis.strategies.integers(0, 10), names=pairs(text_ascii()))
    def test_struct_expr_and_series(
        self, *, length: int, names: tuple[str, str]
    ) -> None:
        name1, name2 = names
        expr = struct(int_range(end=length).alias(name1)).alias(name1)
        series = int_range(end=length, eager=True).alias(name2)
        result = try_reify_expr(expr, series)
        assert isinstance(result, Series)
        expected = series.alias(name1).to_frame().select(struct(name1))[name1]
        assert_series_equal(result, expected)

    # series

    @given(length=hypothesis.strategies.integers(0, 10), name=text_ascii())
    def test_flat_series(self, *, length: int, name: str) -> None:
        series = int_range(end=length, eager=True).alias(name)
        result = try_reify_expr(series)
        assert isinstance(result, Series)
        assert_series_equal(result, series)

    @given(length=hypothesis.strategies.integers(0, 10), names=pairs(text_ascii()))
    def test_flat_series_and_expr(self, *, length: int, names: tuple[str, str]) -> None:
        name1, name2 = names
        series = int_range(end=length, eager=True).alias(name1)
        expr = int_range(end=length).alias(name2)
        result = try_reify_expr(series, expr)
        assert isinstance(result, Series)
        assert_series_equal(result, series)

    @given(length=hypothesis.strategies.integers(0, 10), names=pairs(text_ascii()))
    def test_flat_series_and_series(
        self, *, length: int, names: tuple[str, str]
    ) -> None:
        name1, name2 = names
        series1 = int_range(end=length, eager=True).alias(name1)
        series2 = int_range(end=length, eager=True).alias(name2)
        result = try_reify_expr(series1, series2)
        assert isinstance(result, Series)
        assert_series_equal(result, series1)

    @given(length=hypothesis.strategies.integers(0, 10), name=text_ascii())
    def test_struct_series(self, *, length: int, name: str) -> None:
        series = (
            int_range(end=length, eager=True)
            .alias(name)
            .to_frame()
            .select(struct(name))[name]
        )
        assert isinstance(series.dtype, Struct)
        result = try_reify_expr(series)
        assert isinstance(result, Series)
        assert_series_equal(result, series)

    @given(length=hypothesis.strategies.integers(0, 10), names=pairs(text_ascii()))
    def test_struct_series_and_expr(
        self, *, length: int, names: tuple[str, str]
    ) -> None:
        name1, name2 = names
        series = (
            int_range(end=length, eager=True)
            .alias(name1)
            .to_frame()
            .select(struct(name1))[name1]
        )
        assert isinstance(series.dtype, Struct)
        expr = int_range(end=length).alias(name2)
        result = try_reify_expr(series, expr)
        assert isinstance(result, Series)
        assert_series_equal(result, series)

    @given(length=hypothesis.strategies.integers(0, 10), names=pairs(text_ascii()))
    def test_struct_series_and_series(
        self, *, length: int, names: tuple[str, str]
    ) -> None:
        name1, name2 = names
        series1 = (
            int_range(end=length, eager=True)
            .alias(name1)
            .to_frame()
            .select(struct(name1))[name1]
        )
        assert isinstance(series1.dtype, Struct)
        series2 = int_range(end=length).alias(name2)
        result = try_reify_expr(series1, series2)
        assert isinstance(result, Series)
        assert_series_equal(result, series1)


class TestUniform:
    @given(
        length=hypothesis.strategies.integers(0, 10),
        bounds=pairs(floats(0.0, 1.0), sorted=True),
    )
    def test_int(self, *, length: int, bounds: tuple[float, float]) -> None:
        low, high = bounds
        series = uniform(length, low=low, high=high)
        assert series.len() == length
        assert series.is_between(low, high).all()
        self._assert(series, length, low, high)

    @given(
        length=hypothesis.strategies.integers(0, 10),
        bounds=pairs(floats(0.0, 1.0), sorted=True),
    )
    def test_series(self, *, length: int, bounds: tuple[float, float]) -> None:
        low, high = bounds
        orig = int_range(end=length, eager=True)
        series = uniform(orig, low=low, high=high)
        assert series.len() == length
        assert series.is_between(low, high).all()
        self._assert(series, length, low, high)

    @given(
        length=hypothesis.strategies.integers(0, 10),
        bounds=pairs(floats(0.0, 1.0), sorted=True),
    )
    def test_dataframe(self, *, length: int, bounds: tuple[float, float]) -> None:
        low, high = bounds
        df = int_range(end=length, eager=True).to_frame()
        series = uniform(df, low=low, high=high)
        self._assert(series, length, low, high)

    def _assert(self, series: Series, length: int, low: float, high: float, /) -> None:
        assert series.dtype == Float64
        assert series.len() == length
        assert series.is_between(low, high).all()


class TestUniqueElement:
    def test_main(self) -> None:
        series = Series(
            name="x", values=[[], [1], [1, 2], [1, 2, 3]], dtype=List(Int64)
        )
        result = series.to_frame().with_columns(y=unique_element("x"))["y"]
        expected = Series(name="y", values=[None, 1, None, None], dtype=Int64)
        assert_series_equal(result, expected)


class TestWeekNum:
    @given(
        case=sampled_from([
            (
                "mon",
                list(
                    chain(
                        itertools.repeat(2868, 7),
                        itertools.repeat(2869, 7),
                        itertools.repeat(2870, 7),
                        itertools.repeat(2871, 7),
                        itertools.repeat(2872, 7),
                    )
                ),
            ),
            (
                "tue",
                list(
                    chain(
                        itertools.repeat(2867, 1),
                        itertools.repeat(2868, 7),
                        itertools.repeat(2869, 7),
                        itertools.repeat(2870, 7),
                        itertools.repeat(2871, 7),
                        itertools.repeat(2872, 6),
                    )
                ),
            ),
            (
                "wed",
                list(
                    chain(
                        itertools.repeat(2867, 2),
                        itertools.repeat(2868, 7),
                        itertools.repeat(2869, 7),
                        itertools.repeat(2870, 7),
                        itertools.repeat(2871, 7),
                        itertools.repeat(2872, 5),
                    )
                ),
            ),
            (
                "sat",
                list(
                    chain(
                        itertools.repeat(2867, 5),
                        itertools.repeat(2868, 7),
                        itertools.repeat(2869, 7),
                        itertools.repeat(2870, 7),
                        itertools.repeat(2871, 7),
                        itertools.repeat(2872, 2),
                    )
                ),
            ),
            (
                "sun",
                list(
                    chain(
                        itertools.repeat(2867, 6),
                        itertools.repeat(2868, 7),
                        itertools.repeat(2869, 7),
                        itertools.repeat(2870, 7),
                        itertools.repeat(2871, 7),
                        itertools.repeat(2872, 1),
                    )
                ),
            ),
        ])
    )
    def test_main(self, *, case: tuple[WeekDay, Sequence[int]]) -> None:
        start, exp_values = case
        series = date_range(
            dt.date(2024, 12, 16),  # Mon
            dt.date(2025, 1, 19),  # Sun
            interval="1d",
            eager=True,
        ).alias("date")
        result = series.to_frame().with_columns(wn=week_num("date", start=start))["wn"]
        expected = Series(name="wn", values=exp_values, dtype=Int32)
        assert_series_equal(result, expected)


class TestZonedDateTime:
    @given(time_zone=sampled_from([HongKong, UTC]))
    def test_main(self, *, time_zone: ZoneInfo) -> None:
        dtype = zoned_datetime(time_zone=time_zone)
        assert isinstance(dtype, Datetime)
        assert dtype.time_zone is not None
