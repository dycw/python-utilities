from pathlib import Path
from typing import Any

from hypothesis import assume, given
from hypothesis.strategies import (
    DataObject,
    SearchStrategy,
    booleans,
    data,
    dictionaries,
    floats,
    integers,
    lists,
    none,
    sampled_from,
    tuples,
)
from numpy import float32, nan
from pandas import DataFrame, RangeIndex
from pandas.testing import assert_frame_equal, assert_series_equal
from pytest import mark, param, raises

from utilities.fastparquet import (
    _PARQUET_DTYPES,
    EmptyDataFrameError,
    _get_parquet_file,
    count_rows,
    get_columns,
    get_dtypes,
    read_parquet,
    write_parquet,
)
from utilities.hypothesis import temp_paths, text_ascii
from utilities.hypothesis.pandas import dates_pd
from utilities.numpy import datetime64ns
from utilities.pandas import DataFrameRangeIndexError, Int64, category, string


class TestCountRows:
    @given(rows=lists(booleans(), min_size=1), root=temp_paths())
    def test_main(self, rows: list[bool], root: Path) -> None:
        n = len(rows)
        df = DataFrame(rows, index=RangeIndex(n), columns=["value"])
        write_parquet(df, path := root.joinpath("df.parq"))
        result = count_rows(path)
        assert result == n


class TestGetColumns:
    @given(columns=lists(text_ascii(), unique=True), root=temp_paths())
    def test_main(self, columns: list[str], root: Path) -> None:
        df = DataFrame(nan, index=RangeIndex(1), columns=columns, dtype=float)
        write_parquet(df, path := root.joinpath("df.parq"))
        result = get_columns(path)
        assert result == columns


class TestGetDtypes:
    @given(
        dtypes=dictionaries(text_ascii(), sampled_from(_PARQUET_DTYPES)),
        root=temp_paths(),
    )
    def test_main(self, dtypes: dict[str, Any], root: Path) -> None:
        df = DataFrame(None, index=RangeIndex(1), columns=list(dtypes)).astype(dtypes)
        write_parquet(df, path := root.joinpath("df.parq"))
        result = get_dtypes(path)
        assert result == dtypes


class TestReadAndWriteParquet:
    @given(data=data(), root=temp_paths(), as_series=booleans())
    @mark.parametrize(
        ("elements", "dtype"),
        [
            param(booleans(), bool),
            param(dates_pd() | none(), datetime64ns),
            param(floats(-10.0, 10.0) | none(), float),
            param(integers(-10, 10) | none(), Int64),
            param(text_ascii() | none(), string),
        ],
    )
    def test_main(
        self,
        data: DataObject,
        elements: SearchStrategy[Any],
        dtype: Any,
        root: Path,
        as_series: bool,
    ) -> None:
        rows = data.draw(lists(elements, min_size=1))
        n = len(rows)
        df = DataFrame(rows, index=RangeIndex(n), columns=["value"]).astype(dtype)
        write_parquet(df, path := root.joinpath("df.parq"))
        head = data.draw(sampled_from([n, None]))
        columns = "value" if as_series else None
        read = read_parquet(path, head=head, columns=columns)
        if as_series:
            assert_series_equal(read, df["value"])
        else:
            assert_frame_equal(read, df)

    @given(data=data(), column1=text_ascii(), column2=text_ascii(), root=temp_paths())
    def test_series_from_dataframe_with_two_string_columns(
        self, data: DataObject, column1: str, column2: str, root: Path
    ) -> None:
        _ = assume(column1 != column2)
        elements = text_ascii() | none()
        rows = data.draw(lists(tuples(elements, elements), min_size=1))
        df = DataFrame(
            rows, index=RangeIndex(len(rows)), columns=[column1, column2], dtype=string
        )
        write_parquet(df, path := root.joinpath("df.parq"))
        sr = read_parquet(path, columns=column1)
        assert_series_equal(sr, df[column1])


class TestWriteParquet:
    @given(value=text_ascii(), root=temp_paths())
    def test_strings_are_stored_as_categoricals(self, value: str, root: Path) -> None:
        df = DataFrame(value, index=RangeIndex(1), columns=["value"], dtype=string)
        write_parquet(df, path := root.joinpath("df.parq"))
        file = _get_parquet_file(path)
        dtypes = file.dtypes
        assert dtypes == {"value": category}

    def test_empty_dataframe_error(self, tmp_path: Path) -> None:
        df = DataFrame()
        with raises(EmptyDataFrameError):
            write_parquet(df, tmp_path.joinpath("df.parq"))

    def test_check_range_index(self, tmp_path: Path) -> None:
        df = DataFrame(nan, index=[0], columns=["value"])
        with raises(DataFrameRangeIndexError):
            write_parquet(df, tmp_path.joinpath("df.parq"))

    def test_check_invalid_dtype(self, tmp_path: Path) -> None:
        df = DataFrame(nan, index=RangeIndex(1), columns=["value"], dtype=float32)
        with raises(TypeError):
            write_parquet(df, tmp_path.joinpath("df.parq"))
