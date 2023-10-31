from __future__ import annotations

from polars import DataFrame, Float64
from pytest import raises

from utilities.polars import (
    DataFrameColumnsError,
    DataFrameDTypesError,
    DataFrameHeightError,
    DataFrameSchemaError,
    DataFrameShapeError,
    DataFrameSortedError,
    DataFrameUniqueError,
    DataFrameWidthError,
    check_dataframe,
)


class TestCheckDataFrame:
    def test_main(self) -> None:
        df = DataFrame()
        check_dataframe(df)

    def test_columns_pass(self) -> None:
        df = DataFrame()
        check_dataframe(df, columns=[])

    def test_columns_error(self) -> None:
        df = DataFrame()
        with raises(DataFrameColumnsError):
            check_dataframe(df, columns=["value"])

    def test_dtypes_pass(self) -> None:
        df = DataFrame()
        check_dataframe(df, dtypes=[])

    def test_dtypes_error(self) -> None:
        df = DataFrame()
        with raises(DataFrameDTypesError):
            check_dataframe(df, dtypes=[Float64])

    def test_height_pass(self) -> None:
        df = DataFrame()
        check_dataframe(df, height=0)

    def test_height_error(self) -> None:
        df = DataFrame()
        with raises(DataFrameHeightError):
            check_dataframe(df, height=1)

    def test_schema_pass(self) -> None:
        df = DataFrame()
        check_dataframe(df, schema={})

    def test_schema_error(self) -> None:
        df = DataFrame()
        with raises(DataFrameSchemaError):
            check_dataframe(df, schema={"value": Float64})

    def test_shape_pass(self) -> None:
        df = DataFrame()
        check_dataframe(df, shape=(0, 0))

    def test_shape_error(self) -> None:
        df = DataFrame()
        with raises(DataFrameShapeError):
            check_dataframe(df, shape=(1, 1))

    def test_sorted_pass(self) -> None:
        df = DataFrame({"value": [0.0, 1.0]})
        check_dataframe(df, sorted="value")

    def test_sorted_error(self) -> None:
        df = DataFrame({"value": [1.0, 0.0]})
        with raises(DataFrameSortedError):
            check_dataframe(df, sorted="value")

    def test_unique_pass(self) -> None:
        df = DataFrame({"value": [0.0, 1.0]})
        check_dataframe(df, unique="value")

    def test_unique_error(self) -> None:
        df = DataFrame({"value": [0.0, 0.0]})
        with raises(DataFrameUniqueError):
            check_dataframe(df, unique="value")

    def test_width_pass(self) -> None:
        df = DataFrame()
        check_dataframe(df, width=0)

    def test_width_error(self) -> None:
        df = DataFrame()
        with raises(DataFrameWidthError):
            check_dataframe(df, width=1)
