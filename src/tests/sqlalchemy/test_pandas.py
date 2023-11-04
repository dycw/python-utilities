from __future__ import annotations

import datetime as dt
from collections.abc import Sized
from typing import Any, cast

import sqlalchemy
from hypothesis import given
from hypothesis.strategies import (
    DataObject,
    SearchStrategy,
    booleans,
    data,
    floats,
    integers,
    just,
    lists,
    none,
    sets,
)
from numpy import int64
from pandas import DataFrame, NaT, Series
from pytest import mark, param, raises
from sqlalchemy import (
    Boolean,
    Column,
    Date,
    DateTime,
    Engine,
    Float,
    Integer,
    MetaData,
    String,
    Table,
    select,
)
from sqlalchemy.exc import DuplicateColumnError

from utilities.hypothesis import (
    dates_pd,
    datetimes_pd,
    int64s,
    sqlite_engines,
    text_ascii,
)
from utilities.numpy import datetime64ns
from utilities.pandas import Int64, boolean, datetime64nsutc, string
from utilities.sqlalchemy import (
    DatesWithTimeComponentsError,
    NonPositiveStreamError,
    SeriesAgainstTableColumnError,
    SeriesNameNotInTableError,
    SeriesNameSnakeCaseNotInTableError,
    ensure_tables_created,
    insert_dataframe,
    insert_items,
    select_to_dataframe,
)
from utilities.sqlalchemy.pandas import (
    _check_select_for_duplicates,
    _check_series_against_table_column,
    _dataframe_columns_to_snake,
    _parse_series_against_table,
    _rows_to_dataframe,
    _stream_dataframes,
    _table_column_to_dtype,
    _yield_dataframe_rows_as_dicts,
    _yield_insertion_elements,
)
from utilities.text import snake_case
from utilities.types import NoneType


class TestCheckSelectForDuplicates:
    def test_error(self) -> None:
        table = Table("example", MetaData(), Column("id", Integer, primary_key=True))
        sel = select(table.c.id, table.c.id)
        with raises(DuplicateColumnError):
            _check_select_for_duplicates(sel)


class TestCheckSeriesAgainstTableColumn:
    @mark.parametrize(
        ("dtype", "column_type"),
        [
            param(bool, Boolean),
            param(boolean, Boolean),
            param(bool, Integer),
            param(boolean, Integer),
            param(float, Float),
            param(datetime64ns, Date),
            param(datetime64nsutc, DateTime),
            param(int, Integer),
            param(Int64, Integer),
            param(string, String),
        ],
    )
    def test_success(self, *, dtype: Any, column_type: Any) -> None:
        series = Series([], dtype=dtype)
        column = Column("id", column_type)
        _check_series_against_table_column(series, column)

    @mark.parametrize(
        ("dtype", "column_type"),
        [param(object, Integer), param(datetime64ns, DateTime)],
    )
    def test_error(self, *, dtype: Any, column_type: Any) -> None:
        series = Series([], dtype=dtype)
        column = Column("id", column_type)
        with raises(SeriesAgainstTableColumnError):
            _check_series_against_table_column(series, column)

    def test_allow_naive_datetimes(self) -> None:
        series = Series([], dtype=datetime64ns)
        column = Column("id", DateTime)
        _check_series_against_table_column(series, column, allow_naive_datetimes=True)


class TestDataFrameColumnsToSnake:
    @given(col_name=text_ascii())
    def test_main(self, *, col_name: str) -> None:
        df = DataFrame(columns=[col_name])
        snake = _dataframe_columns_to_snake(df)
        assert snake.columns.tolist() == [snake_case(col_name)]


class TestInsertDataFrame:
    @given(engine=sqlite_engines(), ids=sets(integers(0, 10)))
    def test_main(self, *, engine: Engine, ids: set[int]) -> None:
        table = Table("example", MetaData(), Column("id", Integer, primary_key=True))
        ensure_tables_created(engine, table)
        df = DataFrame(list(ids), columns=["id"], dtype=int)
        insert_dataframe(df, table, engine)
        sel = select(table.c["id"])
        with engine.begin() as conn:
            res = conn.execute(sel).scalars().all()
        assert set(res) == ids


class TestParseSeriesAgainstTable:
    @mark.parametrize(
        ("series_name", "table_column_name", "snake"),
        [
            param("id", "id", False),
            param("id", "Id", True),
            param("Id", "id", True),
        ],
    )
    def test_main(
        self, *, series_name: str, table_column_name: str, snake: bool
    ) -> None:
        series = Series([], dtype=int, name=series_name)
        table = Table(
            "example",
            MetaData(),
            Column(table_column_name, Integer, primary_key=True),
        )
        key, _ = _parse_series_against_table(series, table, snake=snake)
        assert key == table_column_name

    @mark.parametrize(
        ("snake", "error"),
        [
            param(True, SeriesNameSnakeCaseNotInTableError),
            param(False, SeriesNameNotInTableError),
        ],
    )
    def test_error(self, *, snake: bool, error: type[Exception]) -> None:
        series = Series([], dtype=int, name="name")
        table = Table("example", MetaData(), Column("id", Integer, primary_key=True))
        with raises(error):
            _ = _parse_series_against_table(series, table, snake=snake)


class TestRowsToDataFrame:
    @given(engine=sqlite_engines(), ids=sets(integers(0, 10)))
    @mark.parametrize(("col_name", "snake"), [param("id", False), param("Id", True)])
    def test_main(
        self, *, col_name: str, ids: set[int], engine: Engine, snake: bool
    ) -> None:
        table = Table(
            "example", MetaData(), Column(col_name, Integer, primary_key=True)
        )
        ensure_tables_created(engine, table)
        insert_items(engine, ([(id_,) for id_ in ids], table))
        with engine.begin() as conn:
            rows = conn.execute(sel := select(table)).all()
        df = _rows_to_dataframe(sel, rows, snake=snake)
        assert len(df) == len(ids)
        assert dict(df.dtypes) == {"id": Int64}


class TestSelectToDataFrame:
    @given(
        engine=sqlite_engines(),
        ids=sets(integers(min_value=0, max_value=10), min_size=1, max_size=10),
        stream=integers(1, 10) | none(),
    )
    def test_main(self, *, engine: Engine, ids: set[int], stream: int | None) -> None:
        table = Table("example", MetaData(), Column("id", Integer, primary_key=True))
        ensure_tables_created(engine, table)
        insert_items(engine, ([(id_,) for id_ in ids], table))
        result = select_to_dataframe(select(table), engine, stream=stream)
        if stream is None:
            assert isinstance(result, DataFrame)
            assert len(cast(Sized, result)) == len(ids)
            assert dict(cast(Any, result).dtypes) == {"id": Int64}
        else:
            assert not isinstance(result, DataFrame)
            for df in result:
                assert 1 <= len(df) <= stream
                assert dict(df.dtypes) == {"id": Int64}


class TestStreamDataFrames:
    @given(
        engine=sqlite_engines(),
        ids=sets(integers(min_value=0, max_value=10), min_size=1, max_size=10),
        stream=integers(1, 10),
    )
    def test_main(self, *, engine: Engine, ids: set[int], stream: int) -> None:
        table = Table("example", MetaData(), Column("id", Integer, primary_key=True))
        ensure_tables_created(engine, table)
        insert_items(engine, ([(id_,) for id_ in ids], table))
        for df in _stream_dataframes(select(table), engine, stream):
            assert 1 <= len(df) <= stream
            assert dict(df.dtypes) == {"id": Int64}

    @given(engine=sqlite_engines())
    def test_non_positive_stream(self, *, engine: Engine) -> None:
        table = Table("example", MetaData(), Column("id", Integer, primary_key=True))
        with raises(NonPositiveStreamError):
            _ = list(_stream_dataframes(select(table), engine, 0))


class TestTableColumnToDtype:
    @mark.parametrize(
        ("column", "expected"),
        [
            param(Column(Boolean), boolean),
            param(Column(Date), datetime64ns),
            param(Column(DateTime), datetime64ns),
            param(Column(Float), float),
            param(Column(Integer), Int64),
            param(Column(String), string),
            param(Column(sqlalchemy.DECIMAL), float),
        ],
    )
    def test_main(self, *, column: Any, expected: Any) -> None:
        assert _table_column_to_dtype(column) == expected


class TestYieldDataFrameRowsAsDicts:
    @given(data=data())
    @mark.parametrize(
        ("elements", "df_dtype", "table_dtype", "result_type"),
        [
            param(booleans(), bool, Boolean, bool),
            param(booleans() | none(), boolean, Boolean, (bool, NoneType)),
            param(dates_pd(), datetime64ns, Date, dt.date),
            param(
                dates_pd() | just(NaT) | none(),
                datetime64ns,
                Date,
                (dt.date, NoneType),
            ),
            param(datetimes_pd(), datetime64nsutc, DateTime, dt.datetime),
            param(
                datetimes_pd() | just(NaT) | none(),
                datetime64nsutc,
                DateTime,
                (dt.datetime, NoneType),
            ),
            param(floats(), float, Float, (float, NoneType)),
            param(int64s(), int64, Integer, int),
            param(int64s() | none(), Int64, Integer, (int, NoneType)),
            param(text_ascii(), string, String, str),
            param(text_ascii() | none(), string, String, (str, NoneType)),
        ],
    )
    def test_main(
        self,
        data: DataObject,
        elements: SearchStrategy[Any],
        df_dtype: Any,
        table_dtype: Any,
        result_type: Any,
    ) -> None:
        values = data.draw(lists(elements, max_size=10))
        df = DataFrame(values, columns=["id"], dtype=df_dtype)
        table = Table(
            "example", MetaData(), Column("id", table_dtype, primary_key=True)
        )
        dicts = list(_yield_dataframe_rows_as_dicts(df, table))
        assert len(dicts) == len(values)
        for dict_ in dicts:
            assert isinstance(dict_["id"], result_type)


class TestYieldInsertionElements:
    @given(data=data())
    @mark.parametrize(
        ("elements", "dtype", "expected"),
        [
            param(booleans(), bool, bool),
            param(booleans() | none(), boolean, (bool, NoneType)),
            param(dates_pd(), datetime64ns, dt.date),
            param(
                dates_pd() | just(NaT) | none(),
                datetime64ns,
                (dt.date, NoneType),
            ),
            param(datetimes_pd(), datetime64nsutc, dt.datetime),
            param(
                datetimes_pd() | just(NaT) | none(),
                datetime64nsutc,
                (dt.datetime, NoneType),
            ),
            param(floats(), float, (float, NoneType)),
            param(int64s(), int64, int),
            param(int64s() | none(), Int64, (int, NoneType)),
            param(text_ascii(), string, str),
            param(text_ascii() | none(), string, (str, NoneType)),
        ],
    )
    def test_main(
        self,
        data: DataObject,
        elements: SearchStrategy[Any],
        dtype: Any,
        expected: type | tuple[type, ...],
    ) -> None:
        values = data.draw(lists(elements, max_size=10))
        series = Series(values, dtype=dtype)
        for el in _yield_insertion_elements(series):
            assert isinstance(el, expected)

    @given(date=dates_pd())
    def test_dates_with_time_components_error(self, *, date: dt.date) -> None:
        series = Series([dt.datetime.combine(date, dt.time(12))], dtype=datetime64ns)
        with raises(DatesWithTimeComponentsError):
            _ = list(_yield_insertion_elements(series))

    def test_type_error(self) -> None:
        series = Series(dtype=object)
        with raises(TypeError, match="Invalid dtype"):
            _ = list(_yield_insertion_elements(series))
