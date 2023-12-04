from __future__ import annotations

from polars import DataFrame, Float64, Int64, Utf8, concat
from polars.testing import assert_frame_equal
from polars.type_aliases import PolarsDataType
from pytest import mark, param, raises

from utilities.polars import (
    CheckPolarsDataFrameError,
    EmptyPolarsConcatError,
    SetFirstRowAsColumnsError,
    check_polars_dataframe,
    group_by_nan_sum,
    join,
    redirect_empty_polars_concat,
    set_first_row_as_columns,
)


class TestCheckPolarsDataFrame:
    def test_main(self) -> None:
        df = DataFrame()
        check_polars_dataframe(df)

    def test_columns_pass(self) -> None:
        df = DataFrame()
        check_polars_dataframe(df, columns=[])

    def test_columns_error(self) -> None:
        df = DataFrame()
        with raises(CheckPolarsDataFrameError):
            check_polars_dataframe(df, columns=["value"])

    def test_dtypes_pass(self) -> None:
        df = DataFrame()
        check_polars_dataframe(df, dtypes=[])

    def test_dtypes_error(self) -> None:
        df = DataFrame()
        with raises(CheckPolarsDataFrameError):
            check_polars_dataframe(df, dtypes=[Float64])

    def test_height_pass(self) -> None:
        df = DataFrame({"value": [0.0]})
        check_polars_dataframe(df, height=1)

    def test_height_error(self) -> None:
        df = DataFrame({"value": [0.0]})
        with raises(CheckPolarsDataFrameError):
            check_polars_dataframe(df, height=2)

    def test_min_height_pass(self) -> None:
        df = DataFrame({"value": [0.0, 1.0]})
        check_polars_dataframe(df, min_height=1)

    def test_min_height_error(self) -> None:
        df = DataFrame()
        with raises(CheckPolarsDataFrameError):
            check_polars_dataframe(df, min_height=1)

    def test_max_height_pass(self) -> None:
        df = DataFrame()
        check_polars_dataframe(df, max_height=1)

    def test_max_height_error(self) -> None:
        df = DataFrame({"value": [0.0, 1.0]})
        with raises(CheckPolarsDataFrameError):
            check_polars_dataframe(df, max_height=1)

    def test_schema_pass(self) -> None:
        df = DataFrame()
        check_polars_dataframe(df, schema={})

    def test_schema_error(self) -> None:
        df = DataFrame()
        with raises(CheckPolarsDataFrameError):
            check_polars_dataframe(df, schema={"value": Float64})

    def test_shape_pass(self) -> None:
        df = DataFrame()
        check_polars_dataframe(df, shape=(0, 0))

    def test_shape_error(self) -> None:
        df = DataFrame()
        with raises(CheckPolarsDataFrameError):
            check_polars_dataframe(df, shape=(1, 1))

    def test_sorted_pass(self) -> None:
        df = DataFrame({"value": [0.0, 1.0]})
        check_polars_dataframe(df, sorted="value")

    def test_sorted_error(self) -> None:
        df = DataFrame({"value": [1.0, 0.0]})
        with raises(CheckPolarsDataFrameError):
            check_polars_dataframe(df, sorted="value")

    def test_unique_pass(self) -> None:
        df = DataFrame({"value": [0.0, 1.0]})
        check_polars_dataframe(df, unique="value")

    def test_unique_error(self) -> None:
        df = DataFrame({"value": [0.0, 0.0]})
        with raises(CheckPolarsDataFrameError):
            check_polars_dataframe(df, unique="value")

    def test_width_pass(self) -> None:
        df = DataFrame()
        check_polars_dataframe(df, width=0)

    def test_width_error(self) -> None:
        df = DataFrame()
        with raises(CheckPolarsDataFrameError):
            check_polars_dataframe(df, width=1)


class TestGroupByNanSum:
    @mark.parametrize("dtype", [param(Int64), param(Float64)])
    def test_main(self, *, dtype: PolarsDataType) -> None:
        df = DataFrame(
            [
                ("one None", None),
                ("two Nones", None),
                ("two Nones", None),
                ("one int", 1),
                ("one int, one None", 1),
                ("one int, one None", None),
                ("one int, two Nones", 1),
                ("one int, two Nones", None),
                ("one int, two Nones", None),
                ("two ints", 1),
                ("two ints", 2),
                ("two ints, one None", 1),
                ("two ints, one None", 2),
                ("two ints, one None", None),
                ("two ints, two Nones", 1),
                ("two ints, two Nones", 2),
                ("two ints, two Nones", None),
                ("two ints, two Nones", None),
            ],
            schema={"id": Utf8, "value": dtype},
        )
        result = group_by_nan_sum(df, "id", "value")
        expected = DataFrame(
            [
                ("one None", None),
                ("two Nones", None),
                ("one int", 1),
                ("one int, one None", 1),
                ("one int, two Nones", 1),
                ("two ints", 3),
                ("two ints, one None", 3),
                ("two ints, two Nones", 3),
            ],
            schema={"id": Utf8, "value": dtype},
        )
        assert_frame_equal(result.sort("id"), expected.sort("id"))


class TestJoin:
    def test_main(self) -> None:
        df1 = DataFrame([{"a": 1, "b": 2}], schema={"a": Int64, "b": Int64})
        df2 = DataFrame([{"a": 1, "c": 3}], schema={"a": Int64, "c": Int64})
        result = join(df1, df2, on="a")
        expected = DataFrame(
            [{"a": 1, "b": 2, "c": 3}], schema={"a": Int64, "b": Int64, "c": Int64}
        )
        assert_frame_equal(result, expected)


class TestRedirectEmptyPolarsConcat:
    def test_main(self) -> None:
        with raises(EmptyPolarsConcatError), redirect_empty_polars_concat():
            _ = concat([])


class TestSetFirstRowAsColumns:
    def test_empty(self) -> None:
        df = DataFrame()
        with raises(SetFirstRowAsColumnsError):
            _ = set_first_row_as_columns(df)

    def test_one_row(self) -> None:
        df = DataFrame(["value"])
        check_polars_dataframe(df, height=1, schema={"column_0": Utf8})
        result = set_first_row_as_columns(df)
        check_polars_dataframe(result, height=0, schema={"value": Utf8})

    def test_multiple_rows(self) -> None:
        df = DataFrame(["foo", "bar", "baz"])
        check_polars_dataframe(df, height=3, schema={"column_0": Utf8})
        result = set_first_row_as_columns(df)
        check_polars_dataframe(result, height=2, schema={"foo": Utf8})
