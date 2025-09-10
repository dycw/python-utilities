from __future__ import annotations

import datetime as dt
import enum
import itertools
import math
from dataclasses import dataclass, field
from enum import auto
from itertools import chain
from math import isfinite, nan
from random import Random
from typing import TYPE_CHECKING, Any, ClassVar, Literal, assert_never, cast
from uuid import UUID, uuid4

import hypothesis.strategies
import numpy as np
import polars as pl
import whenever
from hypothesis import assume, given
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
)
from numpy import allclose, linspace, pi
from polars import (
    Boolean,
    DataFrame,
    DataType,
    Datetime,
    Duration,
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
from polars.schema import Schema
from polars.testing import assert_frame_equal, assert_series_equal
from pytest import mark, param, raises
from scipy.stats import norm
from whenever import (
    Date,
    DateDelta,
    DateTimeDelta,
    PlainDateTime,
    TimeDelta,
    ZonedDateTime,
)

import tests.test_math
import utilities.polars
from utilities.hypothesis import (
    assume_does_not_raise,
    date_deltas,
    date_periods,
    date_time_deltas,
    dates,
    float64s,
    int64s,
    pairs,
    py_datetimes,
    temp_paths,
    text_ascii,
    time_deltas,
    time_periods,
    times,
    zoned_date_time_periods,
    zoned_date_times,
)
from utilities.numpy import DEFAULT_RNG
from utilities.pathlib import PWD
from utilities.polars import (
    BooleanValueCountsError,
    ColumnsToDictError,
    DatePeriodDType,
    DatetimeHongKong,
    DatetimeTokyo,
    DatetimeUSCentral,
    DatetimeUSEastern,
    DatetimeUTC,
    ExprOrSeries,
    FiniteEWMMeanError,
    InsertAfterError,
    InsertBeforeError,
    OneColumnEmptyError,
    OneColumnNonUniqueError,
    RoundToFloatError,
    SelectExactError,
    SetFirstRowAsColumnsError,
    TimePeriodDType,
    _AppendRowExtraKeysError,
    _AppendRowMissingKeysError,
    _AppendRowNullColumnsError,
    _AppendRowPredicateError,
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
    _GetDataTypeOrSeriesTimeZoneStructNonUniqueError,
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
    all_dataframe_columns,
    all_series,
    any_dataframe_columns,
    any_series,
    append_row,
    are_frames_equal,
    bernoulli,
    boolean_value_counts,
    check_polars_dataframe,
    choice,
    columns_to_dict,
    concat_series,
    convert_time_zone,
    cross,
    cross_rolling_quantile,
    dataclass_to_dataframe,
    dataclass_to_schema,
    decreasing_horizontal,
    deserialize_dataframe,
    deserialize_series,
    ensure_data_type,
    ensure_expr_or_series,
    ensure_expr_or_series_many,
    expr_to_series,
    false_like,
    filter_date,
    filter_time,
    finite_ewm_mean,
    first_true_horizontal,
    get_data_type_or_series_time_zone,
    get_expr_name,
    get_frequency_spectrum,
    increasing_horizontal,
    insert_after,
    insert_before,
    insert_between,
    is_false,
    is_near_event,
    is_true,
    join,
    join_into_periods,
    map_over_columns,
    nan_sum_agg,
    nan_sum_horizontal,
    normal_pdf,
    normal_rv,
    number_of_decimals,
    offset_datetime,
    one_column,
    order_of_magnitude,
    period_range,
    read_dataframe,
    read_series,
    reify_exprs,
    replace_time_zone,
    round_to_float,
    search_period,
    select_exact,
    serialize_dataframe,
    serialize_series,
    set_first_row_as_columns,
    struct_dtype,
    to_false,
    to_not_false,
    to_not_true,
    to_true,
    touch,
    true_like,
    try_reify_expr,
    uniform,
    unique_element,
    week_num,
    write_dataframe,
    write_series,
    zoned_date_time_dtype,
    zoned_date_time_period_dtype,
)
from utilities.sentinel import Sentinel, sentinel
from utilities.tzdata import HongKong, Tokyo, USCentral, USEastern
from utilities.whenever import (
    NOW_UTC,
    TODAY_UTC,
    DatePeriod,
    TimePeriod,
    ZonedDateTimePeriod,
    get_now,
    get_now_plain,
    get_today,
    to_zoned_date_time,
)
from utilities.zoneinfo import UTC, to_time_zone_name

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Mapping, Sequence
    from pathlib import Path
    from zoneinfo import ZoneInfo

    from _pytest.mark import ParameterSet
    from polars._typing import IntoExprColumn, PolarsDataType, SchemaDict

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


class TestAnyAllDataFrameColumnsSeries:
    cases: ClassVar[list[ParameterSet]] = [
        param(int_range(end=pl.len()) % 2 == 0),
        param(int_range(end=4, eager=True) % 2 == 0),
    ]
    series: ClassVar[Series] = Series(
        name="x", values=[True, True, False, False], dtype=Boolean
    )
    df: ClassVar[DataFrame] = series.to_frame()
    exp_all: ClassVar[Series] = Series(
        name="x", values=[True, False, False, False], dtype=Boolean
    )
    exp_any: ClassVar[Series] = Series(
        name="x", values=[True, True, True, False], dtype=Boolean
    )
    exp_empty: ClassVar[Series] = Series(
        name="x", values=[True, False, True, False], dtype=Boolean
    )

    @mark.parametrize("column", cases)
    def test_df_all(self, *, column: ExprOrSeries) -> None:
        result = all_dataframe_columns(self.df, "x", column)
        assert_series_equal(result, self.exp_all)

    @mark.parametrize("column", cases)
    def test_df_any(self, *, column: ExprOrSeries) -> None:
        result = any_dataframe_columns(self.df, "x", column)
        assert_series_equal(result, self.exp_any)

    @mark.parametrize("column", cases)
    def test_df_all_empty(self, *, column: ExprOrSeries) -> None:
        result = all_dataframe_columns(self.df, column.alias("x"))
        assert_series_equal(result, self.exp_empty)

    @mark.parametrize("column", cases)
    def test_df_any_empty(self, *, column: ExprOrSeries) -> None:
        result = any_dataframe_columns(self.df, column.alias("x"))
        assert_series_equal(result, self.exp_empty)

    @mark.parametrize("column", cases)
    def test_series_all(self, *, column: ExprOrSeries) -> None:
        result = all_series(self.series, column)
        assert_series_equal(result, self.exp_all)

    @mark.parametrize("column", cases)
    def test_series_any_any_any(self, *, column: ExprOrSeries) -> None:
        result = any_series(self.series, column)
        assert_series_equal(result, self.exp_any)


class TestAppendRow:
    rows: ClassVar[list[tuple[int, int]]] = [(i, 2 * i) for i in range(3)]
    schema: ClassVar[SchemaDict] = {"x": Int64, "y": Int64}
    df: ClassVar[DataFrame] = DataFrame(data=rows, schema=schema, orient="row")

    @mark.parametrize(
        "row", [param({"x": 3, "y": 6}), param({"x": 3, "y": 6, "z": None})]
    )
    def test_main(self, *, row: StrMapping) -> None:
        result = append_row(self.df, row)
        expected = DataFrame(
            data=[*self.rows, (3, 6)], schema=self.schema, orient="row"
        )
        assert_frame_equal(result, expected)

    def test_missing_key(self) -> None:
        row = {"x": 3}
        result = append_row(self.df, row)
        expected = DataFrame(
            data=[*self.rows, (3, None)], schema=self.schema, orient="row"
        )
        assert_frame_equal(result, expected)

    def test_disallow_missing_selected(self) -> None:
        row = {"x": 3}
        result = append_row(self.df, row, disallow_missing="x")
        expected = DataFrame(
            data=[*self.rows, (3, None)], schema=self.schema, orient="row"
        )
        assert_frame_equal(result, expected)

    def test_disallow_null_selected(self) -> None:
        row = {"x": 3, "y": None}
        result = append_row(self.df, row, disallow_null="x")
        expected = DataFrame(
            data=[*self.rows, (3, None)], schema=self.schema, orient="row"
        )
        assert_frame_equal(result, expected)

    def test_in_place(self) -> None:
        df = DataFrame(data=self.rows, schema=self.schema, orient="row")
        assert df.height == 3
        row = {"x": 3, "y": 6}
        _ = append_row(df, row, in_place=True)
        assert df.height == 4

    def test_error_predicate(self) -> None:
        row = {"x": 3}
        with raises(_AppendRowPredicateError, match=r"Predicate failed; got {'x': 3}"):
            _ = append_row(self.df, row, predicate=lambda row: "y" in row)

    def test_error_disallow_extra(self) -> None:
        row = {"x": 3, "y": 6, "z": None}
        with raises(_AppendRowExtraKeysError, match=r"Extra key\(s\) found; got {'z'}"):
            _ = append_row(self.df, row, disallow_extra=True)

    def test_error_disallow_missing_all(self) -> None:
        with raises(
            _AppendRowMissingKeysError,
            match=r"Missing key\(s\) found; got {'[xy]', '[xy]'}",
        ):
            _ = append_row(self.df, {}, disallow_missing=True)

    def test_error_disallow_missing_selected(self) -> None:
        with raises(
            _AppendRowMissingKeysError, match=r"Missing key\(s\) found; got {'x'}"
        ):
            _ = append_row(self.df, {}, disallow_missing="x")

    def test_error_disallow_null_all(self) -> None:
        row = {"x": None, "y": None}
        with raises(
            _AppendRowNullColumnsError,
            match=r"Null column\(s\) found; got {'[xy]', '[xy]'}",
        ):
            _ = append_row(self.df, row, disallow_null=True)

    def test_error_disallow_null_selected(self) -> None:
        row = {"x": None, "y": None}
        with raises(
            _AppendRowNullColumnsError, match=r"Null column\(s\) found; got {'x'}"
        ):
            _ = append_row(self.df, row, disallow_null="x")


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
            BooleanValueCountsError, match=r"Column 'z' must be Boolean; got Int64"
        ):
            _ = boolean_value_counts(self.df, col("x").cast(Int64).alias("z"))


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
            match=r"DataFrame must have columns .*; got .*:\n\n.*",
        ):
            check_polars_dataframe(df, columns=["value"])

    def test_dtypes_pass(self) -> None:
        df = DataFrame()
        check_polars_dataframe(df, dtypes=[])

    def test_dtypes_error(self) -> None:
        df = DataFrame()
        with raises(
            _CheckPolarsDataFrameDTypesError,
            match=r"DataFrame must have dtypes .*; got .*\n\n.*",
        ):
            check_polars_dataframe(df, dtypes=[Float64])

    def test_height_pass(self) -> None:
        df = DataFrame(data={"value": [0.0]})
        check_polars_dataframe(df, height=1)

    def test_height_error(self) -> None:
        df = DataFrame(data={"value": [0.0]})
        with raises(
            _CheckPolarsDataFrameHeightError,
            match=r"DataFrame must satisfy the height requirements; got .*:\n\n.*",
        ):
            check_polars_dataframe(df, height=2)

    def test_min_height_pass(self) -> None:
        df = DataFrame(data={"value": [0.0, 1.0]})
        check_polars_dataframe(df, min_height=1)

    def test_min_height_error(self) -> None:
        df = DataFrame()
        with raises(
            _CheckPolarsDataFrameHeightError,
            match=r"DataFrame must satisfy the height requirements; got .*:\n\n.*",
        ):
            check_polars_dataframe(df, min_height=1)

    def test_max_height_pass(self) -> None:
        df = DataFrame()
        check_polars_dataframe(df, max_height=1)

    def test_max_height_error(self) -> None:
        df = DataFrame(data={"value": [0.0, 1.0]})
        with raises(
            _CheckPolarsDataFrameHeightError,
            match=r"DataFrame must satisfy the height requirements; got .*:\n\n.*",
        ):
            check_polars_dataframe(df, max_height=1)

    def test_predicates_pass(self) -> None:
        df = DataFrame(data={"value": [0.0, 1.0]})
        check_polars_dataframe(df, predicates={"value": isfinite})

    def test_predicates_error_missing_columns_and_failed(self) -> None:
        df = DataFrame(data={"a": [0.0, nan], "b": [0.0, nan]})
        with raises(
            _CheckPolarsDataFramePredicatesError,
            match=r"DataFrame must satisfy the predicates; missing columns were .* and failed predicates were .*:\n\n.*",
        ):
            check_polars_dataframe(df, predicates={"a": isfinite, "c": isfinite})

    def test_predicates_error_missing_columns_only(self) -> None:
        df = DataFrame()
        with raises(
            _CheckPolarsDataFramePredicatesError,
            match=r"DataFrame must satisfy the predicates; missing columns were .*:\n\n.*",
        ):
            check_polars_dataframe(df, predicates={"a": isfinite})

    def test_predicates_error_failed_only(self) -> None:
        df = DataFrame(data={"a": [0.0, nan]})
        with raises(
            _CheckPolarsDataFramePredicatesError,
            match=r"DataFrame must satisfy the predicates; failed predicates were .*:\n\n.*",
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
            match=r"DataFrame must have shape .*; got .*:\n\n.*",
        ):
            check_polars_dataframe(df, shape=(1, 1))

    def test_sorted_pass(self) -> None:
        df = DataFrame(data={"value": [0.0, 1.0]})
        check_polars_dataframe(df, sorted="value")

    def test_sorted_error(self) -> None:
        df = DataFrame(data={"value": [1.0, 0.0]})
        with raises(
            _CheckPolarsDataFrameSortedError,
            match=r"DataFrame must be sorted on .*:\n\n.*",
        ):
            check_polars_dataframe(df, sorted="value")

    def test_unique_pass(self) -> None:
        df = DataFrame(data={"value": [0.0, 1.0]})
        check_polars_dataframe(df, unique="value")

    def test_unique_error(self) -> None:
        df = DataFrame(data={"value": [0.0, 0.0]})
        with raises(
            _CheckPolarsDataFrameUniqueError,
            match=r"DataFrame must be unique on .*:\n\n.*",
        ):
            check_polars_dataframe(df, unique="value")

    def test_width_pass(self) -> None:
        df = DataFrame()
        check_polars_dataframe(df, width=0)

    def test_width_error(self) -> None:
        df = DataFrame()
        with raises(
            _CheckPolarsDataFrameWidthError,
            match=r"DataFrame must have width .*; got .*:\n\n.*",
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
            match=r"DataFrame must satisfy the predicates; (missing columns|failed predicates) were .*:\n\n.*",
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


class TestColumnsToDict:
    schema: ClassVar[SchemaDict] = {"x": Int64, "y": Int64}

    @mark.parametrize("x", [param("x"), param(col.x)])
    @mark.parametrize("y", [param("y"), param(col.y)])
    def test_main(self, *, x: IntoExprColumn, y: IntoExprColumn) -> None:
        df = DataFrame(data=[(1, 2), (3, 4), (5, 6)], schema=self.schema, orient="row")
        mapping = columns_to_dict(df, x, y)
        expected = {1: 2, 3: 4, 5: 6}
        assert mapping == expected

    def test_error(self) -> None:
        df = DataFrame(data=[(1, 1), (1, 2), (1, 3)], schema=self.schema, orient="row")
        with raises(
            ColumnsToDictError, match=r"DataFrame must be unique on 'x':\n\n.*"
        ):
            _ = columns_to_dict(df, "x", "y")


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
            },
        )

    @given(data=data())
    def test_date(self, *, data: DataObject) -> None:
        @dataclass(kw_only=True, slots=True)
        class Example:
            x: whenever.Date

        objs = data.draw(lists(builds(Example, x=dates()), min_size=1))
        df = dataclass_to_dataframe(objs)
        check_polars_dataframe(df, height=len(objs), schema_list={"x": pl.Date})

    @given(data=data())
    def test_date_delta(self, *, data: DataObject) -> None:
        @dataclass(kw_only=True, slots=True)
        class Example:
            x: DateDelta

        objs = data.draw(lists(builds(Example, x=date_deltas()), min_size=1))
        df = dataclass_to_dataframe(objs)
        check_polars_dataframe(df, height=len(objs), schema_list={"x": Duration})

    @given(data=data())
    def test_date_period(self, *, data: DataObject) -> None:
        @dataclass(kw_only=True, slots=True)
        class Example:
            x: DatePeriod

        objs = data.draw(lists(builds(Example, x=date_periods()), min_size=1))
        df = dataclass_to_dataframe(objs, globalns=globals())
        check_polars_dataframe(df, height=len(objs), schema_list={"x": DatePeriodDType})

    @given(data=data())
    def test_date_time_delta(self, *, data: DataObject) -> None:
        @dataclass(kw_only=True, slots=True)
        class Example:
            x: DateTimeDelta

        objs = data.draw(
            lists(builds(Example, x=date_time_deltas(nativable=True)), min_size=1)
        )
        df = dataclass_to_dataframe(objs)
        check_polars_dataframe(df, height=len(objs), schema_list={"x": Duration})

    @given(data=data())
    def test_multiple_periods(self, *, data: DataObject) -> None:
        @dataclass(kw_only=True, slots=True)
        class Example:
            x: DatePeriod
            y: DatePeriod

        objs = data.draw(
            lists(builds(Example, x=date_periods(), y=date_periods()), min_size=1)
        )
        df = dataclass_to_dataframe(objs, globalns=globals())
        check_polars_dataframe(
            df,
            height=len(objs),
            schema_list={"x": DatePeriodDType, "y": DatePeriodDType},
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
    def test_time(self, *, data: DataObject) -> None:
        @dataclass(kw_only=True, slots=True)
        class Example:
            x: whenever.Time

        objs = data.draw(lists(builds(Example, x=times()), min_size=1))
        df = dataclass_to_dataframe(objs)
        check_polars_dataframe(df, height=len(objs), schema_list={"x": pl.Time})

    @given(data=data())
    def test_time_delta(self, *, data: DataObject) -> None:
        @dataclass(kw_only=True, slots=True)
        class Example:
            x: TimeDelta

        objs = data.draw(lists(builds(Example, x=time_deltas()), min_size=1))
        df = dataclass_to_dataframe(objs)
        check_polars_dataframe(df, height=len(objs), schema_list={"x": Duration})

    @given(data=data())
    def test_time_period(self, *, data: DataObject) -> None:
        @dataclass(kw_only=True, slots=True)
        class Example:
            x: TimePeriod

        objs = data.draw(lists(builds(Example, x=time_periods()), min_size=1))
        df = dataclass_to_dataframe(objs, globalns=globals())
        check_polars_dataframe(df, height=len(objs), schema_list={"x": TimePeriodDType})

    @given(data=data())
    def test_uuid(self, *, data: DataObject) -> None:
        @dataclass(kw_only=True, slots=True)
        class Example:
            x: UUID = field(default_factory=uuid4)

        obj = data.draw(builds(Example))
        df = dataclass_to_dataframe(obj, localns=locals())
        check_polars_dataframe(df, height=len(df), schema_list={"x": String})

    @given(data=data())
    def test_zoned_datetime(self, *, data: DataObject) -> None:
        @dataclass(kw_only=True, slots=True)
        class Example:
            x: ZonedDateTime

        objs = data.draw(lists(builds(Example, x=zoned_date_times()), min_size=1))
        df = dataclass_to_dataframe(objs, localns=locals())
        check_polars_dataframe(
            df, height=len(objs), schema_list={"x": zoned_date_time_dtype()}
        )

    @given(data=data())
    def test_zoned_datetime_period(self, *, data: DataObject) -> None:
        @dataclass(kw_only=True, slots=True)
        class Example:
            x: ZonedDateTimePeriod

        objs = data.draw(
            lists(builds(Example, x=zoned_date_time_periods()), min_size=1)
        )
        df = dataclass_to_dataframe(objs, globalns=globals())
        check_polars_dataframe(
            df, height=len(objs), schema_list={"x": zoned_date_time_period_dtype()}
        )

    def test_error_empty(self) -> None:
        with raises(
            _DataClassToDataFrameEmptyError,
            match=r"At least 1 dataclass must be given; got 0",
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
            match=r"Iterable .* must contain exactly 1 class; got .*, .* and perhaps more",
        ):
            _ = dataclass_to_dataframe([Example1(), Example2()])


class TestDataClassToSchema:
    def test_basic(self) -> None:
        @dataclass(kw_only=True, slots=True)
        class Example:
            bool_field: bool = False
            int_field: int = 0
            float_field: float = 0.0
            str_field: str = ""

        obj = Example()
        result = dataclass_to_schema(obj)
        expected = {
            "bool_field": Boolean,
            "int_field": Int64,
            "float_field": Float64,
            "str_field": String,
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

    def test_date(self) -> None:
        @dataclass(kw_only=True, slots=True)
        class Example:
            x: whenever.Date = field(default_factory=get_today)

        obj = Example()
        result = dataclass_to_schema(obj)
        expected = {"x": Object}
        assert result == expected

    def test_date_period(self) -> None:
        @dataclass(kw_only=True, slots=True)
        class Example:
            x: DatePeriod

        obj = Example(x=DatePeriod(TODAY_UTC, TODAY_UTC))
        result = dataclass_to_schema(obj, globalns=globals())
        expected = {"x": Object}
        assert result == expected

    def test_date_delta(self) -> None:
        @dataclass(kw_only=True, slots=True)
        class Example:
            x: DateDelta = field(default_factory=DateDelta)

        obj = Example()
        result = dataclass_to_schema(obj)
        expected = {"x": Object}
        assert result == expected

    def test_date_time_delta(self) -> None:
        @dataclass(kw_only=True, slots=True)
        class Example:
            x: DateTimeDelta = field(default_factory=DateTimeDelta)

        obj = Example()
        result = dataclass_to_schema(obj)
        expected = {"x": Object}
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

    def test_plain_date_time(self) -> None:
        @dataclass(kw_only=True, slots=True)
        class Example:
            x: PlainDateTime = field(default_factory=get_now_plain)

        obj = Example()
        result = dataclass_to_schema(obj)
        expected = {"x": Object}
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

        obj = Example()
        result = dataclass_to_schema(obj)
        expected = {"x": Object}
        assert result == expected

    def test_time(self) -> None:
        @dataclass(kw_only=True, slots=True)
        class Example:
            x: whenever.Time = field(default_factory=whenever.Time)

        obj = Example()
        result = dataclass_to_schema(obj)
        expected = {"x": Object}
        assert result == expected

    def test_time_delta(self) -> None:
        @dataclass(kw_only=True, slots=True)
        class Example:
            x: TimeDelta = field(default_factory=TimeDelta)

        obj = Example()
        result = dataclass_to_schema(obj)
        expected = {"x": Object}
        assert result == expected

    def test_time_period(self) -> None:
        @dataclass(kw_only=True, slots=True)
        class Example:
            x: TimePeriod

        obj = Example(x=TimePeriod(whenever.Time(), whenever.Time()))
        result = dataclass_to_schema(obj, globalns=globals())
        expected = {"x": Object}
        assert result == expected

    def test_uuid(self) -> None:
        @dataclass(kw_only=True, slots=True)
        class Example:
            x: UUID = field(default_factory=uuid4)

        obj = Example()
        result = dataclass_to_schema(obj)
        expected = {"x": Object}
        assert result == expected

    def test_zoned_date_time(self) -> None:
        @dataclass(kw_only=True, slots=True)
        class Example:
            x: ZonedDateTime = field(default_factory=get_now)

        obj = Example()
        result = dataclass_to_schema(obj)
        expected = {"x": Object}
        assert result == expected

    def test_zoned_date_time_period(self) -> None:
        @dataclass(kw_only=True, slots=True)
        class Example:
            x: ZonedDateTimePeriod

        obj = Example(x=ZonedDateTimePeriod(NOW_UTC, NOW_UTC))
        result = dataclass_to_schema(obj, globalns=globals())
        expected = {"x": Object}
        assert result == expected

    def test_error(self) -> None:
        @dataclass(kw_only=True, slots=True)
        class Example:
            x: Sentinel = sentinel

        obj = Example()
        with raises(NotImplementedError):
            _ = dataclass_to_schema(obj)


class TestDatetimeDTypes:
    @mark.parametrize(
        ("time_zone", "dtype"),
        [
            param(HongKong, DatetimeHongKong),
            param(Tokyo, DatetimeTokyo),
            param(USCentral, DatetimeUSCentral),
            param(USEastern, DatetimeUSEastern),
            param(UTC, DatetimeUTC),
        ],
    )
    def test_main(self, *, time_zone: ZoneInfo, dtype: Datetime) -> None:
        name = to_time_zone_name(time_zone)
        expected = dtype.time_zone
        assert name == expected


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


class TestExprToSeries:
    def test_main(self) -> None:
        expr = int_range(end=10)
        series = expr_to_series(expr)
        expected = int_range(end=10, eager=True)
        assert_series_equal(series, expected)


class TestComputeDateFilter:
    def test_main(self) -> None:
        series = datetime_range(
            start=ZonedDateTime(2024, 1, 1, tz=UTC.key).py_datetime(),
            end=ZonedDateTime(2024, 1, 4, 12, tz=UTC.key).py_datetime(),
            interval="12h",
            eager=True,
        ).alias("datetime")
        assert len(series) == 8
        result = filter_date(series, include=[Date(2024, 1, 2), Date(2024, 1, 3)])
        expected = Series(
            name="datetime",
            values=[False, False, True, True, True, True, False, False],
            dtype=Boolean,
        )
        assert_series_equal(result, expected)


class TestComputeTimeFilter:
    def test_main(self) -> None:
        series = datetime_range(
            start=ZonedDateTime(2024, 1, 1, tz=UTC.key).py_datetime(),
            end=ZonedDateTime(2024, 1, 3, 0, tz=UTC.key).py_datetime(),
            interval="6h",
            eager=True,
        ).alias("datetime")
        assert len(series) == 9
        result = filter_time(series, include=[(whenever.Time(6), whenever.Time(12))])
        expected = Series(
            name="datetime",
            values=[False, True, True, False, False, True, True, False, False],
            dtype=Boolean,
        )
        assert_series_equal(result, expected)


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

    @mark.parametrize(
        ("alpha", "exp_base", "min_weight", "exp_result"),
        [
            param(
                0.75,
                alpha_0_75_values,
                0.9,
                [-8.28235294117647, -8.070588235294117, 3.2705882352941176],
            ),
            param(
                0.75,
                alpha_0_75_values,
                0.9999,
                [-8.269864158112174, -8.06746317849418, 3.2331284833087284],
            ),
            param(
                0.99,
                alpha_0_99_values,
                0.9,
                [-8.970002970002971, -8.00970200970201, 6.849915849915851],
            ),
            param(
                0.99,
                alpha_0_99_values,
                0.9999,
                [-8.970002000096999, -8.00970002000097, 6.84990300128499],
            ),
        ],
    )
    def test_main(
        self,
        *,
        alpha: float,
        exp_base: list[float],
        min_weight: float,
        exp_result: list[float],
    ) -> None:
        state = Random(0)
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


class TestFirstTrueHorizontal:
    @mark.parametrize(
        ("x", "y", "z", "expected"),
        [
            param(True, True, True, 0),
            param(False, True, True, 1),
            param(False, False, True, 2),
            param(False, False, False, None),
        ],
    )
    def test_main(self, *, x: bool, y: bool, z: bool, expected: int | None) -> None:
        series = [Series(values=[i], dtype=Boolean) for i in [x, y, z]]
        result = first_true_horizontal(*series)
        assert result.item() == expected


class TestGetDataTypeOrSeriesTimeZone:
    @given(
        time_zone=sampled_from([HongKong, UTC]),
        flat_or_struct=sampled_from(["flat", "struct"]),
        dtype_or_series=sampled_from(["dtype", "series"]),
    )
    def test_main(
        self,
        *,
        time_zone: ZoneInfo,
        flat_or_struct: Literal["flat", "struct"],
        dtype_or_series: Literal["dtype", "series"],
    ) -> None:
        match flat_or_struct:
            case "flat":
                dtype = zoned_date_time_dtype(time_zone=time_zone)
            case "struct":
                dtype = zoned_date_time_period_dtype(time_zone=time_zone)
            case never:
                assert_never(never)
        match dtype_or_series:
            case "dtype":
                obj = dtype
            case "series":
                obj = Series(dtype=dtype)
            case never:
                assert_never(never)
        result = get_data_type_or_series_time_zone(obj)
        assert result is time_zone

    def test_error_not_datetime(self) -> None:
        with raises(
            _GetDataTypeOrSeriesTimeZoneNotDateTimeError,
            match=r"Data type must be Datetime; got Boolean",
        ):
            _ = get_data_type_or_series_time_zone(Boolean)

    def test_error_not_zoned(self) -> None:
        with raises(
            _GetDataTypeOrSeriesTimeZoneNotZonedError,
            match=r"Data type must be zoned; got .*",
        ):
            _ = get_data_type_or_series_time_zone(Datetime)

    def test_error_struct_non_unique(self) -> None:
        with raises(
            _GetDataTypeOrSeriesTimeZoneStructNonUniqueError,
            match=r"Struct data type must contain exactly one time zone; got .*, .* and perhaps more",
        ):
            _ = get_data_type_or_series_time_zone(
                struct_dtype(start=DatetimeHongKong, end=DatetimeUTC)
            )


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


class TestIncreasingAndDecreasingHorizontal:
    def test_main(self) -> None:
        df = DataFrame(
            data=[(1, 2, 3), (1, 3, 2), (2, 1, 3), (2, 3, 1), (3, 1, 2), (3, 2, 1)],
            schema={"x": Int64, "y": Int64, "z": Int64},
            orient="row",
        ).with_columns(
            increasing_horizontal("x", "y", "z").alias("inc"),
            decreasing_horizontal("x", "y", "z").alias("dec"),
        )
        inc = Series(
            name="inc", values=[True, False, False, False, False, False], dtype=Boolean
        )
        assert_series_equal(df["inc"], inc)
        dec = Series(
            name="dec", values=[False, False, False, False, False, True], dtype=Boolean
        )
        assert_series_equal(df["dec"], dec)

    def test_empty(self) -> None:
        df = (
            Series(name="x", values=[1, 2, 3], dtype=Int64)
            .to_frame()
            .with_columns(
                increasing_horizontal().alias("inc"),
                decreasing_horizontal().alias("dec"),
            )
        )
        expected = Series(values=[True, True, True], dtype=Boolean)
        assert_series_equal(df["inc"], expected, check_names=False)
        assert_series_equal(df["dec"], expected, check_names=False)


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
        with raises(error, match=r"DataFrame must have column 'missing'; got .*"):
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
            match=r"DataFrame must have columns 'x' and 'y'; got .*",
        ):
            _ = insert_between(self.df, "x", "y", lit(None).alias("new"))

    def test_error_non_consecutive(self) -> None:
        with raises(
            _InsertBetweenNonConsecutiveError,
            match=r"DataFrame columns 'a' and 'c' must be consecutive; got indices 0 and 2",
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


class TestIsClose:
    @given(values=pairs(float64s()), rel_tol=floats(0.0, 1.0), abs_tol=floats(0.0, 1.0))
    def test_main(
        self, *, values: tuple[float, float], rel_tol: float, abs_tol: float
    ) -> None:
        x, y = values
        x_sr, y_sr = [Series(values=[i], dtype=Float64) for i in values]
        result = utilities.polars.is_close(x_sr, y_sr, rel_tol=rel_tol, abs_tol=abs_tol)
        expected = math.isclose(x, y, rel_tol=rel_tol, abs_tol=abs_tol)
        assert result.item() is expected


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
            name="z",
            values=list(itertools.repeat(object=False, times=10)),
            dtype=Boolean,
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


class TestIsTrueAndFalse:
    series: ClassVar[Series] = Series(
        name="x", values=[True, False, None], dtype=Boolean
    )

    def test_true(self) -> None:
        result = is_true(self.series)
        expected = Series(name="x", values=[True, False, False], dtype=Boolean)
        assert_series_equal(result, expected)

    def test_false(self) -> None:
        result = is_false(self.series)
        expected = Series(name="x", values=[False, True, False], dtype=Boolean)
        assert_series_equal(result, expected)


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
            match=r"Either 'on' must be given or 'left_on' and 'right_on' must be given; got None, 'datetime' and None",
        ):
            _ = join_into_periods(DataFrame(), DataFrame(), left_on="datetime")

    def test_error_periods(self) -> None:
        times = [(dt.time(1), dt.time())]
        df = self._lift_df(times)
        with raises(
            _JoinIntoPeriodsPeriodError,
            match=r"Left DataFrame column 'datetime' must contain valid periods",
        ):
            _ = join_into_periods(df, DataFrame())

    def test_error_left_start_sorted(self) -> None:
        times = [(dt.time(1), dt.time(2)), (dt.time(), dt.time(1))]
        df = self._lift_df(times)
        with raises(
            _JoinIntoPeriodsSortedError,
            match=r"Left DataFrame column 'datetime/start' must be sorted",
        ):
            _ = join_into_periods(df, df)

    def test_error_end_sorted(self) -> None:
        times = [(dt.time(), dt.time(3)), (dt.time(1), dt.time(2))]
        df = self._lift_df(times)
        with raises(
            _JoinIntoPeriodsSortedError,
            match=r"Left DataFrame column 'datetime/end' must be sorted",
        ):
            _ = join_into_periods(df, df)

    def test_error_overlapping(self) -> None:
        times = [(dt.time(), dt.time(2)), (dt.time(1), dt.time(3))]
        df = self._lift_df(times)
        with raises(
            _JoinIntoPeriodsOverlappingError,
            match=r"Left DataFrame column 'datetime' must not contain overlaps",
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
    @mark.parametrize(
        ("values", "expected"),
        [
            param([None], None),
            param([None, None], None),
            param([0], 0),
            param([0, None], 0),
            param([0, None, None], 0),
            param([1, 2], 3),
            param([1, 2, None], 3),
            param([1, 2, None, None], 3),
        ],
    )
    def test_main(self, *, values: list[int | None], expected: int | None) -> None:
        df = (
            Series(name="x", values=values, dtype=Int64)
            .to_frame()
            .with_columns(id=lit("id"))
        )
        result = df.group_by("id").agg(nan_sum_agg("x"))
        assert result.item(0, "x") == expected


class TestNanSumHorizontal:
    @mark.parametrize(
        ("x", "y", "z", "expected"),
        [
            param(1, 2, 3, 6),
            param(None, 2, 3, 5),
            param(None, None, 3, 3),
            param(None, None, None, None),
        ],
    )
    def test_main(
        self, *, x: int | None, y: int | None, z: int | None, expected: int | None
    ) -> None:
        series = [Series(values=[i], dtype=Int64) for i in [x, y, z]]
        result = nan_sum_horizontal(*series)
        assert result.item() == expected


class TestNormalPDF:
    @given(
        xs=lists(float64s(), max_size=10),
        loc=float64s(),
        scale=float64s(min_value=0.0, exclude_min=True),
    )
    def test_main(self, *, xs: list[float], loc: float, scale: float) -> None:
        x = Series(name="x", values=xs, dtype=Float64)
        series = normal_pdf(x, loc=loc, scale=scale)
        _ = assume(series.is_finite().all())
        with assume_does_not_raise(
            RuntimeWarning, match=r"overflow encountered in (subtract|square|divide)"
        ):
            expected = norm.pdf(xs, loc=loc, scale=scale)
        assert allclose(series, expected)


class TestNormalRV:
    @given(length=hypothesis.strategies.integers(0, 10))
    def test_int(self, *, length: int) -> None:
        series = normal_rv(length)
        self._assert(series, length)

    @given(length=hypothesis.strategies.integers(0, 10))
    def test_series(self, *, length: int) -> None:
        orig = int_range(end=length, eager=True)
        series = normal_rv(orig)
        self._assert(series, length)

    @given(length=hypothesis.strategies.integers(0, 10))
    def test_dataframe(self, *, length: int) -> None:
        df = int_range(end=length, eager=True).to_frame()
        series = normal_rv(df)
        self._assert(series, length)

    def _assert(self, series: Series, length: int, /) -> None:
        assert series.dtype == Float64
        assert series.len() == length
        assert series.is_finite().all()


class TestNumberOfDecimals:
    @given(
        integer=hypothesis.strategies.integers(
            -tests.test_math.TestNumberOfDecimals.max_int,
            tests.test_math.TestNumberOfDecimals.max_int,
        ),
        case=sampled_from(tests.test_math.TestNumberOfDecimals.cases),
    )
    def test_main(self, *, integer: int, case: tuple[float, int]) -> None:
        frac, expected = case
        x = integer + frac
        series = Series(name="x", values=[x], dtype=Float64)
        result = number_of_decimals(series)
        assert result.item() == expected


class TestOffsetDateTime:
    @mark.parametrize(
        ("n", "time"), [param(1, whenever.Time(13, 30)), param(2, whenever.Time(15))]
    )
    def test_main(self, *, n: int, time: whenever.Time) -> None:
        datetime = ZonedDateTime(2000, 1, 1, 12, tz=UTC.key)
        result = offset_datetime(datetime, "1h30m", n=n)
        expected = datetime.replace_time(time)
        assert result == expected


class TestOneColumn:
    def test_main(self) -> None:
        series = int_range(end=10, eager=True).alias("x")
        df = series.to_frame()
        result = one_column(df)
        assert_series_equal(result, series)

    def test_error_empty(self) -> None:
        with raises(OneColumnEmptyError, match=r"DataFrame must not be empty"):
            _ = one_column(DataFrame())

    def test_error_non_unique(self) -> None:
        x, y = [int_range(end=10, eager=True).alias(name) for name in ["x", "y"]]
        df = concat_series(x, y)
        with raises(
            OneColumnNonUniqueError,
            match=r"DataFrame must contain exactly one column; got 'x', 'y' and perhaps more",
        ):
            _ = one_column(df)


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


class TestPeriodRange:
    start: ClassVar[ZonedDateTime] = ZonedDateTime(2000, 1, 1, 12, tz=UTC.key)
    end: ClassVar[ZonedDateTime] = ZonedDateTime(2000, 1, 1, 15, tz=UTC.key)

    @mark.parametrize("end_or_length", [param(end), param(3)])
    def test_main(self, *, end_or_length: ZonedDateTime | int) -> None:
        rng = period_range(self.start, end_or_length, interval="1h", eager=True)
        assert len(rng) == 3
        assert rng.dtype == zoned_date_time_period_dtype()
        assert rng[0]["start"] == self.start.py_datetime()
        assert rng[-1]["end"] == self.end.py_datetime()


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
            _ReifyExprsEmptyError,
            match=r"At least 1 Expression or Series must be given",
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


class TestRoundToFloat:
    @mark.parametrize(("x", "y", "exp_value"), tests.test_math.TestRoundToFloat.cases)
    def test_main(self, *, x: float, y: float, exp_value: float) -> None:
        x_sr = Series(name="x", values=[x], dtype=Float64)
        result = round_to_float(x_sr, y)
        expected = Series(name="x", values=[exp_value], dtype=Float64)
        assert_series_equal(result, expected, check_exact=True)

    def test_dataframe_name(self) -> None:
        df = (
            Series(name="x", values=[1.234], dtype=Float64)
            .to_frame()
            .with_columns(round_to_float("x", 0.1))
        )
        expected = Series(name="x", values=[1.2], dtype=Float64).to_frame()
        assert_frame_equal(df, expected)

    @mark.parametrize(("y_value", "expected"), [param(0.1, 1.2), param(None, None)])
    def test_series_and_expr(self, *, y_value: float, expected: float | None) -> None:
        x = Series(name="x", values=[1.234], dtype=Float64)
        y = lit(y_value, dtype=Float64).alias("y")
        result = round_to_float(x, y)
        assert result.item() == expected

    def test_expr_and_expr(self) -> None:
        x = lit(1.234, dtype=Float64).alias("x")
        y = Series(name="y", values=[0.1], dtype=Float64)
        result = round_to_float(x, y)
        assert result.item() == 1.2

    @mark.parametrize(
        ("x", "y"),
        [param("x", "y"), param(col.x, "y"), param("x", col.y), param(col.x, col.y)],
    )
    def test_error(self, *, x: IntoExprColumn, y: IntoExprColumn) -> None:
        with raises(
            RoundToFloatError,
            match=r"At least 1 of the dividend and/or divisor must be a Series; got .* and .*",
        ):
            _ = round_to_float(cast("Any", x), cast("Any", y))


class TestSearchPeriod:
    @mark.parametrize(
        ("time", "exp_start", "exp_end"),
        [
            param(whenever.Time(8, 58), None, None),
            param(whenever.Time(8, 59), None, None),
            param(whenever.Time(9), 0, None),
            param(whenever.Time(9, 1), 0, 0),
            param(whenever.Time(9, 59), 0, 0),
            param(whenever.Time(10), 1, 0),
            param(whenever.Time(10, 1), 1, 1),
            param(whenever.Time(10, 59), 1, 1),
            param(whenever.Time(11), 2, 1),
            param(whenever.Time(11, 1), 2, 2),
            param(whenever.Time(11, 59), 2, 2),
            param(whenever.Time(12), None, 2),
            param(whenever.Time(12, 1), None, None),
            param(whenever.Time(12, 59), None, None),
            param(whenever.Time(13), 3, None),
            param(whenever.Time(13, 1), 3, 3),
            param(whenever.Time(13, 59), 3, 3),
            param(whenever.Time(14), None, 3),
            param(whenever.Time(14, 1), None, None),
            param(whenever.Time(14, 59), None, None),
            param(whenever.Time(15), 4, None),
            param(whenever.Time(15, 1), 4, 4),
            param(whenever.Time(15, 59), 4, 4),
            param(whenever.Time(16), None, 4),
            param(whenever.Time(16, 1), None, None),
            param(whenever.Time(16, 2), None, None),
        ],
    )
    @mark.parametrize("start_or_end", [param("start"), param("end")])
    def test_main(
        self,
        *,
        time: whenever.Time,
        start_or_end: Literal["start", "end"],
        exp_start: int | None,
        exp_end: int | None,
    ) -> None:
        date = whenever.Date(2000, 1, 1)
        sr = DataFrame(
            data=[
                (
                    date.at(whenever.Time(s)).py_datetime(),
                    date.at(whenever.Time(e)).py_datetime(),
                )
                for s, e in [(9, 10), (10, 11), (11, 12), (13, 14), (15, 16)]
            ],
            schema={"start": DatetimeUTC, "end": DatetimeUTC},
            orient="row",
        ).with_columns(datetime=struct("start", "end"))["datetime"]
        assert len(sr) == 5
        date_time = date.at(time).assume_tz(UTC.key)
        match start_or_end:
            case "start":
                expected = exp_start
            case "end":
                expected = exp_end
            case never:
                assert_never(never)
        index = search_period(sr, date_time, start_or_end=start_or_end)
        if expected is None:
            assert index is None
        else:
            assert index is not None
            assert 0 <= index <= (len(sr) - 1)
            start, end = map(
                to_zoned_date_time, cast("Iterable[dt.datetime]", sr[index].values())
            )
            match start_or_end:
                case "start":
                    assert start <= date_time < end
                case "end":
                    assert start < date_time <= end
                case never:
                    assert_never(never)
            if index > 0:
                prev_end = to_zoned_date_time(cast("dt.datetime", sr[index - 1]["end"]))
                assert prev_end <= date_time
            if index < (len(sr) - 1):
                next_start = to_zoned_date_time(
                    cast("dt.datetime", sr[index + 1]["start"])
                )
                assert date_time <= next_start


class TestSelectExact:
    df: ClassVar[DataFrame] = DataFrame(
        data=[(True, False), (False, None), (None, True)],
        schema={"x": Boolean, "y": Boolean},
        orient="row",
    )

    def test_adding_and_reordering(self) -> None:
        df = select_exact(self.df, "y", ~col.x.alias("z"), "x")
        expected = DataFrame(
            data=[(False, False, True), (None, True, False), (True, None, None)],
            schema={"y": Boolean, "z": Boolean, "x": Boolean},
            orient="row",
        )
        assert_frame_equal(df, expected)

    def test_adding_and_dropping(self) -> None:
        df = select_exact(self.df, "y", ~col.x.alias("z"), drop="x")
        expected = DataFrame(
            data=[(False, False), (None, True), (True, None)],
            schema={"y": Boolean, "z": Boolean},
            orient="row",
        )
        assert_frame_equal(df, expected)

    def test_error(self) -> None:
        with raises(
            SelectExactError,
            match=r"All columns must be selected; got \['y'\] remaining",
        ):
            _ = select_exact(self.df, ~col.x.alias("z"), "x")


class TestSerializeAndDeserializeDataFrame:
    cases: ClassVar[list[tuple[PolarsDataType, SearchStrategy[Any]]]] = [
        (Boolean, booleans()),
        (Boolean(), booleans()),
        (pl.Date, hypothesis.strategies.dates()),
        (pl.Date(), hypothesis.strategies.dates()),
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
            match=r"DataFrame must have at least 1 row; got .*",
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


class TestToTrueAndFalse:
    series_tt: ClassVar[Series] = Series(name="x", values=[True, True], dtype=Boolean)
    series_tf: ClassVar[Series] = Series(name="x", values=[True, False], dtype=Boolean)
    series_t0: ClassVar[Series] = Series(name="x", values=[True, None], dtype=Boolean)
    series_ft: ClassVar[Series] = Series(name="x", values=[False, True], dtype=Boolean)
    series_ff: ClassVar[Series] = Series(name="x", values=[False, False], dtype=Boolean)
    series_f0: ClassVar[Series] = Series(name="x", values=[False, None], dtype=Boolean)
    series_0t: ClassVar[Series] = Series(name="x", values=[None, True], dtype=Boolean)
    series_0f: ClassVar[Series] = Series(name="x", values=[None, False], dtype=Boolean)
    series_00: ClassVar[Series] = Series(name="x", values=[None, None], dtype=Boolean)

    @mark.parametrize(
        ("series", "exp_values"),
        [
            param(series_tt, [False, False]),
            param(series_tf, [False, False]),
            param(series_t0, [False, False]),
            param(series_ft, [False, True]),
            param(series_ff, [False, False]),
            param(series_f0, [False, False]),
            param(series_0t, [False, True]),
            param(series_0f, [False, False]),
            param(series_00, [False, False]),
        ],
    )
    def test_to_true(self, *, series: Series, exp_values: list[bool]) -> None:
        result = to_true(series)
        exp_series = Series(name="x", values=exp_values, dtype=Boolean)
        assert_series_equal(result, exp_series)

    @mark.parametrize(
        ("series", "exp_values"),
        [
            param(series_tt, [False, False]),
            param(series_tf, [False, True]),
            param(series_t0, [False, True]),
            param(series_ft, [False, False]),
            param(series_ff, [False, False]),
            param(series_f0, [False, False]),
            param(series_0t, [False, False]),
            param(series_0f, [False, False]),
            param(series_00, [False, False]),
        ],
    )
    def test_to_not_true(self, *, series: Series, exp_values: list[bool]) -> None:
        result = to_not_true(series)
        exp_series = Series(name="x", values=exp_values, dtype=Boolean)
        assert_series_equal(result, exp_series)

    @mark.parametrize(
        ("series", "exp_values"),
        [
            param(series_tt, [False, False]),
            param(series_tf, [False, True]),
            param(series_t0, [False, False]),
            param(series_ft, [False, False]),
            param(series_ff, [False, False]),
            param(series_f0, [False, False]),
            param(series_0t, [False, False]),
            param(series_0f, [False, True]),
            param(series_00, [False, False]),
        ],
    )
    def test_to_false(self, *, series: Series, exp_values: list[bool]) -> None:
        result = to_false(series)
        exp_series = Series(name="x", values=exp_values, dtype=Boolean)
        assert_series_equal(result, exp_series)

    @mark.parametrize(
        ("series", "exp_values"),
        [
            param(series_tt, [False, False]),
            param(series_tf, [False, False]),
            param(series_t0, [False, False]),
            param(series_ft, [False, True]),
            param(series_ff, [False, False]),
            param(series_f0, [False, True]),
            param(series_0t, [False, False]),
            param(series_0f, [False, False]),
            param(series_00, [False, False]),
        ],
    )
    def test_to_not_false(self, *, series: Series, exp_values: list[bool]) -> None:
        result = to_not_false(series)
        exp_series = Series(name="x", values=exp_values, dtype=Boolean)
        assert_series_equal(result, exp_series)


class TestTrueLikeAndFalseLike:
    @given(length=hypothesis.strategies.integers(0, 10), name=text_ascii())
    def test_true_expr(self, *, length: int, name: str) -> None:
        expr = int_range(end=length).alias(name)
        result = true_like(expr)
        assert isinstance(result, Expr)
        result2 = (
            int_range(end=length, eager=True)
            .alias(f"_{name}")
            .to_frame()
            .with_columns(result)[name]
        )
        expected = pl.repeat(value=True, n=length, dtype=Boolean, eager=True).alias(
            name
        )
        assert_series_equal(result2, expected)

    @given(length=hypothesis.strategies.integers(0, 10), name=text_ascii())
    def test_true_series(self, *, length: int, name: str) -> None:
        series = int_range(end=length, eager=True).alias(name)
        result = true_like(series)
        assert isinstance(result, Series)
        expected = pl.repeat(value=True, n=length, dtype=Boolean, eager=True).alias(
            name
        )
        assert_series_equal(result, expected)

    @given(length=hypothesis.strategies.integers(0, 10), name=text_ascii())
    def test_false_expr(self, *, length: int, name: str) -> None:
        expr = int_range(end=length).alias(name)
        result = false_like(expr)
        assert isinstance(result, Expr)
        result2 = (
            int_range(end=length, eager=True)
            .alias(f"_{name}")
            .to_frame()
            .with_columns(result)[name]
        )
        expected = pl.repeat(value=False, n=length, dtype=Boolean, eager=True).alias(
            name
        )
        assert_series_equal(result2, expected)

    @given(length=hypothesis.strategies.integers(0, 10), name=text_ascii())
    def test_false_series(self, *, length: int, name: str) -> None:
        series = int_range(end=length, eager=True).alias(name)
        result = false_like(series)
        assert isinstance(result, Series)
        expected = pl.repeat(value=False, n=length, dtype=Boolean, eager=True).alias(
            name
        )
        assert_series_equal(result, expected)


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


class TestZonedDateTimeDType:
    def test_main(self) -> None:
        dtype = zoned_date_time_dtype(time_zone=UTC)
        assert isinstance(dtype, Datetime)
        assert dtype.time_zone is not None


class TestZonedDateTimePeriodDType:
    @given(time_zone=sampled_from([UTC, (UTC, UTC)]))
    def test_main(self, *, time_zone: ZoneInfo | tuple[ZoneInfo, ZoneInfo]) -> None:
        dtype = zoned_date_time_period_dtype(time_zone=time_zone)
        assert isinstance(dtype, Struct)
