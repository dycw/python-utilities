from __future__ import annotations

import datetime as dt
import enum
from dataclasses import dataclass, field
from enum import auto
from math import isfinite, nan
from pathlib import Path
from re import escape
from typing import TYPE_CHECKING, Any, ClassVar, Literal, cast
from uuid import UUID, uuid4

import polars as pl
from hypothesis import assume, given
from hypothesis.strategies import (
    DataObject,
    builds,
    data,
    fixed_dictionaries,
    floats,
    integers,
    lists,
    none,
    sampled_from,
    timezones,
)
from polars import (
    Boolean,
    DataFrame,
    Date,
    Datetime,
    Expr,
    Float64,
    Int64,
    List,
    Object,
    Series,
    Struct,
    Utf8,
    col,
    datetime_range,
    int_range,
    lit,
)
from polars.testing import assert_frame_equal, assert_series_equal
from pytest import mark, param, raises

from utilities.datetime import get_now, get_today
from utilities.hypothesis import int64s, text_ascii, zoned_datetimes
from utilities.math import is_greater_than, is_less_than, is_positive
from utilities.pathlib import PWD
from utilities.polars import (
    AppendDataClassError,
    ColumnsToDictError,
    DatetimeHongKong,
    DatetimeTokyo,
    DatetimeUSCentral,
    DatetimeUSEastern,
    DatetimeUTC,
    DropNullStructSeriesError,
    IsNotNullStructSeriesError,
    IsNullStructSeriesError,
    RollingParametersExponential,
    RollingParametersSimple,
    SetFirstRowAsColumnsError,
    StructFromDataClassError,
    YieldStructSeriesElementsError,
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
    _GetDataTypeOrSeriesTimeZoneNotDatetimeError,
    _GetDataTypeOrSeriesTimeZoneNotZonedError,
    _RollingParametersArgumentsError,
    _RollingParametersMinPeriodsError,
    _yield_struct_series_element_remove_nulls,
    _YieldRowsAsDataClassesColumnsSuperSetError,
    _YieldRowsAsDataClassesWrongTypeError,
    append_dataclass,
    are_frames_equal,
    ceil_datetime,
    check_polars_dataframe,
    collect_series,
    columns_to_dict,
    convert_time_zone,
    dataclass_to_dataframe,
    dataclass_to_schema,
    drop_null_struct_series,
    ensure_expr_or_series,
    floor_datetime,
    get_data_type_or_series_time_zone,
    is_not_null_struct_series,
    is_null_struct_series,
    join,
    map_over_columns,
    nan_sum_agg,
    nan_sum_cols,
    replace_time_zone,
    rolling_parameters,
    set_first_row_as_columns,
    struct_dtype,
    struct_from_dataclass,
    unique_element,
    yield_rows_as_dataclasses,
    yield_struct_series_dataclasses,
    yield_struct_series_elements,
    zoned_datetime,
)
from utilities.sentinel import Sentinel, sentinel
from utilities.zoneinfo import (
    UTC,
    HongKong,
    Tokyo,
    USCentral,
    USEastern,
    get_time_zone_name,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping
    from zoneinfo import ZoneInfo

    from polars._typing import IntoExprColumn, PolarsDataType, SchemaDict

    from utilities.types import StrMapping


TruthLit = Literal["true", "false"]  # in 3.12, use type TruthLit = ...


class TestAppendDataClass:
    @given(
        data=fixed_dictionaries({
            "a": int64s() | none(),
            "b": floats() | none(),
            "c": text_ascii() | none(),
        })
    )
    def test_columns_and_fields_equal(self, *, data: StrMapping) -> None:
        df = DataFrame(schema={"a": Int64, "b": Float64, "c": Utf8})

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
        df = DataFrame(schema={"a": Int64, "b": Float64, "c": Utf8})

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

    @given(data=fixed_dictionaries({"datetime": zoned_datetimes()}))
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


class TestCeilDatetime:
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


class TestConvertTimeZone:
    def test_datetime(self) -> None:
        now_utc = get_now()
        series = Series(values=[now_utc], dtype=DatetimeUTC)
        result = convert_time_zone(series, time_zone=HongKong)
        expected = Series(values=[now_utc.astimezone(HongKong)], dtype=DatetimeHongKong)
        assert_series_equal(result, expected)

    def test_non_datetime(self) -> None:
        series = Series(values=[True], dtype=Boolean)
        result = convert_time_zone(series, time_zone=HongKong)
        assert_series_equal(result, series)


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
                "str_field": Utf8,
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
        check_polars_dataframe(df, height=len(df), schema_list={"x": Utf8})

    @given(data=data())
    def test_uuid(self, *, data: DataObject) -> None:
        @dataclass(kw_only=True, slots=True)
        class Example:
            x: UUID = field(default_factory=uuid4)

        obj = data.draw(builds(Example))
        df = dataclass_to_dataframe(obj, localns=locals())
        check_polars_dataframe(df, height=len(df), schema_list={"x": Utf8})

    @given(data=data(), time_zone=timezones())
    def test_zoned_datetime(self, *, data: DataObject, time_zone: ZoneInfo) -> None:
        @dataclass(kw_only=True, slots=True)
        class Example:
            x: dt.datetime

        objs = data.draw(
            lists(builds(Example, x=zoned_datetimes(time_zone=time_zone)), min_size=1)
        )
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
            match="Iterable .* must contain exactly one class; got .*, .* and perhaps more",
        ):
            _ = dataclass_to_dataframe([Example1(), Example2()])


class TestDataClassToSchema:
    def test_basic(self) -> None:
        today = get_today()

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
            "str_field": Utf8,
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
        now = get_now().replace(tzinfo=None)

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
        now = get_now(time_zone=time_zone)

        @dataclass(kw_only=True, slots=True)
        class Example:
            x: dt.datetime = now

        obj = Example()
        result = dataclass_to_schema(obj)
        expected = {"x": zoned_datetime(time_zone=time_zone)}
        assert result == expected

    @given(start=timezones(), end=timezones())
    def test_zoned_datetime_nested(self, *, start: ZoneInfo, end: ZoneInfo) -> None:
        now_start = get_now(time_zone=start)
        now_end = get_now(time_zone=end)

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
        result = get_data_type_or_series_time_zone(dtype_or_series)
        assert result is time_zone

    def test_error_not_datetime(self) -> None:
        with raises(
            _GetDataTypeOrSeriesTimeZoneNotDatetimeError,
            match="Data type must be Datetime; got Boolean",
        ):
            _ = get_data_type_or_series_time_zone(Boolean())

    def test_error_not_zoned(self) -> None:
        with raises(
            _GetDataTypeOrSeriesTimeZoneNotZonedError,
            match="Data type must be zoned; got .*",
        ):
            _ = get_data_type_or_series_time_zone(Datetime())


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
        expected = Series(values=exp_values, dtype=Boolean)
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
        expected = Series(values=exp_values, dtype=Boolean)
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
        df1 = DataFrame(data=[{"a": 1, "b": 2}], schema={"a": Int64, "b": Int64})
        df2 = DataFrame(data=[{"a": 1, "c": 3}], schema={"a": Int64, "c": Int64})
        result = join(df1, df2, on="a")
        expected = DataFrame(
            data=[{"a": 1, "b": 2, "c": 3}], schema={"a": Int64, "b": Int64, "c": Int64}
        )
        assert_frame_equal(result, expected)


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
            data=[(x, y)], schema={"x": Int64, "y": Int64}, orient="row"
        ).with_columns(z=nan_sum_cols(x_use, y_use))
        assert df["z"].item() == expected


class TestReplaceTimeZone:
    def test_datetime(self) -> None:
        now_utc = get_now()
        series = Series(values=[now_utc], dtype=DatetimeUTC)
        result = replace_time_zone(series, time_zone=None)
        expected = Series(values=[now_utc.replace(tzinfo=None)], dtype=Datetime)
        assert_series_equal(result, expected)

    def test_non_datetime(self) -> None:
        series = Series(name="series", values=[True], dtype=Boolean)
        result = replace_time_zone(series, time_zone=None)
        assert_series_equal(result, series)


class TestRollingParameters:
    @given(s_window=integers())
    def test_simple(self, *, s_window: int) -> None:
        params = rolling_parameters(s_window=s_window)
        assert isinstance(params, RollingParametersSimple)

    @given(e_com=floats(0.0, 10.0), min_periods=integers(1, 10))
    def test_exponential_com(self, *, e_com: float, min_periods: int) -> None:
        _ = assume(is_positive(e_com, abs_tol=1e-8))
        params = rolling_parameters(e_com=e_com, min_periods=min_periods)
        assert isinstance(params, RollingParametersExponential)

    @given(e_span=floats(1.0, 10.0), min_periods=integers(1, 10))
    def test_exponential_span(self, *, e_span: float, min_periods: int) -> None:
        _ = assume(is_greater_than(e_span, 1.0, abs_tol=1e-8))
        params = rolling_parameters(e_span=e_span, min_periods=min_periods)
        assert isinstance(params, RollingParametersExponential)

    @given(e_half_life=floats(0.0, 10.0), min_periods=integers(1, 10))
    def test_exponential_half_life(
        self, *, e_half_life: float, min_periods: int
    ) -> None:
        _ = assume(is_positive(e_half_life, abs_tol=1e-8))
        params = rolling_parameters(e_half_life=e_half_life, min_periods=min_periods)
        assert isinstance(params, RollingParametersExponential)

    @given(e_alpha=floats(0.0, 1.0), min_periods=integers(1, 10))
    def test_exponential_alpha(self, *, e_alpha: float, min_periods: int) -> None:
        _ = assume(is_positive(e_alpha, abs_tol=1e-8))
        _ = assume(is_less_than(e_alpha, 1.0, abs_tol=1e-8))
        params = rolling_parameters(e_alpha=e_alpha, min_periods=min_periods)
        assert isinstance(params, RollingParametersExponential)

    @given(
        e_com=floats(0.0, 10.0) | none(),
        e_span=floats(0.0, 10.0) | none(),
        e_half_life=floats(0.0, 10.0) | none(),
        e_alpha=floats(0.0, 1.0) | none(),
    )
    def test_error_min_periods(
        self,
        *,
        e_com: float | None,
        e_span: float | None,
        e_half_life: float | None,
        e_alpha: float | None,
    ) -> None:
        _ = assume(
            (e_com is not None)
            or (e_span is not None)
            or (e_half_life is not None)
            or (e_alpha is not None)
        )
        with raises(
            _RollingParametersMinPeriodsError,
            match="Exponential rolling requires 'min_periods' to be set; got None",
        ):
            _ = rolling_parameters(
                e_com=e_com, e_span=e_span, e_half_life=e_half_life, e_alpha=e_alpha
            )

    def test_error_argument(self) -> None:
        with raises(
            _RollingParametersArgumentsError,
            match=escape(
                r"Exactly one of simple window, exponential center of mass (γ), exponential span (θ), exponential half-life (λ) or exponential smoothing factor (α) must be given; got s_window=None, γ=None, θ=None, λ=None and α=None"  # noqa: RUF001
            ),
        ):
            _ = rolling_parameters()


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
        check_polars_dataframe(df, height=1, schema_list={"column_0": Utf8})
        result = set_first_row_as_columns(df)
        check_polars_dataframe(result, height=0, schema_list={"value": Utf8})

    def test_multiple_rows(self) -> None:
        df = DataFrame(data=["foo", "bar", "baz"])
        check_polars_dataframe(df, height=3, schema_list={"column_0": Utf8})
        result = set_first_row_as_columns(df)
        check_polars_dataframe(result, height=2, schema_list={"foo": Utf8})


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
            "str_": Utf8,
            "str_maybe": Utf8,
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
        expected = Struct({"field": Utf8})
        assert result == expected

    def test_literal(self) -> None:
        LowOrHigh = Literal["low", "high"]  # noqa: N806

        @dataclass(kw_only=True, slots=True)
        class Example:
            field: LowOrHigh  # pyright: ignore[reportInvalidTypeForm]

        result = struct_from_dataclass(Example, localns=locals())
        expected = Struct({"field": Utf8})
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
            _ = struct_from_dataclass(cast(Any, None))

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


class TestUniqueElement:
    def test_main(self) -> None:
        series = Series(
            name="x", values=[[], [1], [1, 2], [1, 2, 3]], dtype=List(Int64)
        )
        result = series.to_frame().with_columns(y=unique_element("x"))["y"]
        expected = Series(name="y", values=[None, 1, None, None], dtype=Int64)
        assert_series_equal(result, expected)


class TestYieldRowsAsDataclasses:
    @given(check_types=sampled_from(["none", "first", "all"]))
    def test_main(self, *, check_types: Literal["none", "first", "all"]) -> None:
        df = DataFrame(data=[(1,), (2,), (3,)], schema={"x": Int64}, orient="row")

        @dataclass(kw_only=True, slots=True)
        class Row:
            x: int

        result = list(yield_rows_as_dataclasses(df, Row, check_types=check_types))
        expected = [Row(x=1), Row(x=2), Row(x=3)]
        assert result == expected

    def test_none(self) -> None:
        df = DataFrame(data=[(1,), (2,), (3,)], schema={"x": int}, orient="row")

        @dataclass(kw_only=True, slots=True)
        class Row:
            x: str

        result = list(yield_rows_as_dataclasses(df, Row, check_types="none"))
        expected = [Row(x=cast(Any, 1)), Row(x=cast(Any, 2)), Row(x=cast(Any, 3))]
        assert result == expected

    def test_first(self) -> None:
        df = DataFrame(data=[(1,), (None,), (None,)], schema={"x": int}, orient="row")

        @dataclass(kw_only=True, slots=True)
        class Row:
            x: int

        result = list(yield_rows_as_dataclasses(df, Row, check_types="first"))
        expected = [Row(x=1), Row(x=cast(Any, None)), Row(x=cast(Any, None))]
        assert result == expected

    @given(check_types=sampled_from(["none", "first", "all"]))
    def test_missing_columns_for_fields_with_defaults(
        self, *, check_types: Literal["none", "first", "all"]
    ) -> None:
        df = DataFrame(data=[(1,), (2,), (3,)], schema={"x": Int64}, orient="row")

        @dataclass(kw_only=True, slots=True)
        class Row:
            x: int
            y: int | None = None

        result = list(yield_rows_as_dataclasses(df, Row, check_types=check_types))
        expected = [Row(x=1), Row(x=2), Row(x=3)]
        assert result == expected

    @given(check_types=sampled_from(["none", "first", "all"]))
    def test_literal(self, *, check_types: Literal["none", "first", "all"]) -> None:
        df = DataFrame(
            data=[("true",), ("false",), ("true",)], schema={"x": Utf8}, orient="row"
        )

        @dataclass(kw_only=True, slots=True)
        class Row:
            x: Literal["true", "false"]

        result = list(yield_rows_as_dataclasses(df, Row, check_types=check_types))
        expected = [Row(x="true"), Row(x="false"), Row(x="true")]
        assert result == expected

    @given(check_types=sampled_from(["none", "first", "all"]))
    def test_literal_nullable(
        self, *, check_types: Literal["none", "first", "all"]
    ) -> None:
        df = DataFrame(
            data=[("true",), ("false",), (None,)], schema={"x": Utf8}, orient="row"
        )

        @dataclass(kw_only=True, slots=True)
        class Row:
            x: Literal["true", "false"] | None = None

        result = list(yield_rows_as_dataclasses(df, Row, check_types=check_types))
        expected = [Row(x="true"), Row(x="false"), Row()]
        assert result == expected

    @given(check_types=sampled_from(["none", "first", "all"]))
    def test_literal_type(
        self, *, check_types: Literal["none", "first", "all"]
    ) -> None:
        df = DataFrame(
            data=[("true",), ("false",), ("true",)], schema={"x": Utf8}, orient="row"
        )

        @dataclass(kw_only=True, slots=True)
        class Row:
            x: TruthLit

        result = list(
            yield_rows_as_dataclasses(
                df, Row, check_types=check_types, globalns=globals()
            )
        )
        expected = [Row(x="true"), Row(x="false"), Row(x="true")]
        assert result == expected

    @given(check_types=sampled_from(["none", "first", "all"]))
    def test_literal_type_nullable(
        self, *, check_types: Literal["none", "first", "all"]
    ) -> None:
        df = DataFrame(
            data=[("true",), ("false",), (None,)], schema={"x": Utf8}, orient="row"
        )

        @dataclass(kw_only=True, slots=True)
        class Row:
            x: TruthLit | None = None

        result = list(
            yield_rows_as_dataclasses(
                df, Row, globalns=globals(), check_types=check_types
            )
        )
        expected = [Row(x="true"), Row(x="false"), Row()]
        assert result == expected

    @given(check_types=sampled_from(["none", "first", "all"]))
    def test_empty(self, *, check_types: Literal["none", "first", "all"]) -> None:
        df = DataFrame(data=[], schema={"x": Int64}, orient="row")

        @dataclass(kw_only=True, slots=True)
        class Row:
            x: int

        result = list(yield_rows_as_dataclasses(df, Row, check_types=check_types))
        expected = []
        assert result == expected

    @given(check_types=sampled_from(["none", "first", "all"]))
    def test_error_superset(
        self, *, check_types: Literal["none", "first", "all"]
    ) -> None:
        df = DataFrame(data=[(1,), (2,), (3,)], schema={"x": Int64}, orient="row")

        @dataclass(kw_only=True, slots=True)
        class Row:
            y: int

        with raises(
            _YieldRowsAsDataClassesColumnsSuperSetError,
            match="DataFrame columns .* must be a superset of dataclass fields .*; dataclass had extra fields .*",
        ):
            _ = list(yield_rows_as_dataclasses(df, Row, check_types=check_types))

    @given(check_types=sampled_from(["first", "all"]))
    def test_error_first_or_all_wrong_type(
        self, *, check_types: Literal["first", "all"]
    ) -> None:
        df = DataFrame(data=[(1,), (2,), (3,)], schema={"x": Int64}, orient="row")

        @dataclass(kw_only=True, slots=True)
        class Row:
            x: str

        with raises(
            _YieldRowsAsDataClassesWrongTypeError,
            match='wrong value type for field "x" - should be "str" instead of value "1" of type "int"',
        ):
            _ = list(yield_rows_as_dataclasses(df, Row, check_types=check_types))

    def test_error_all_wrong_type(self) -> None:
        df = DataFrame(data=[(1,), (None,), (3,)], schema={"x": Int64}, orient="row")

        @dataclass(kw_only=True, slots=True)
        class Row:
            x: int

        with raises(
            _YieldRowsAsDataClassesWrongTypeError,
            match='wrong value type for field "x" - should be "int" instead of value "None" of type "NoneType"',
        ):
            _ = list(yield_rows_as_dataclasses(df, Row, check_types="all"))


class TestYieldStructSeriesDataclasses:
    def test_main(self) -> None:
        @dataclass(kw_only=True, slots=True)
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
        @dataclass(kw_only=True, slots=True)
        class Inner:
            lower: int
            upper: int

        @dataclass(kw_only=True, slots=True)
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
        result = list(
            yield_struct_series_dataclasses(
                series, Outer, forward_references={"Inner": Inner}
            )
        )
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
