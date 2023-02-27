import datetime as dt
from collections.abc import Sized
from typing import Any
from typing import Optional
from typing import Union
from typing import cast

import sqlalchemy
from hypothesis import given
from hypothesis.strategies import DataObject
from hypothesis.strategies import SearchStrategy
from hypothesis.strategies import booleans
from hypothesis.strategies import data
from hypothesis.strategies import floats
from hypothesis.strategies import integers
from hypothesis.strategies import just
from hypothesis.strategies import lists
from hypothesis.strategies import none
from hypothesis_sqlalchemy.sample import table_records_lists
from pandas import DataFrame
from pandas import NaT
from pandas import Series
from pytest import mark
from pytest import param
from pytest import raises
from sqlalchemy import Boolean
from sqlalchemy import Column
from sqlalchemy import Date
from sqlalchemy import DateTime
from sqlalchemy import Engine
from sqlalchemy import Float
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import String
from sqlalchemy import Table
from sqlalchemy import func
from sqlalchemy import insert
from sqlalchemy import select
from sqlalchemy.exc import DuplicateColumnError
from sqlalchemy.orm import declarative_base

from utilities.hypothesis import text_ascii
from utilities.hypothesis.numpy import int64s
from utilities.hypothesis.pandas import dates_pd
from utilities.hypothesis.pandas import datetimes_pd
from utilities.hypothesis.sqlalchemy import sqlite_engines
from utilities.numpy import datetime64ns
from utilities.pandas import Int64
from utilities.pandas import boolean
from utilities.pandas import datetime64nsutc
from utilities.pandas import string
from utilities.sqlalchemy import ensure_table_created
from utilities.sqlalchemy import get_table
from utilities.sqlalchemy.pandas import DatesWithTimeComponentsError
from utilities.sqlalchemy.pandas import NonPositiveStreamError
from utilities.sqlalchemy.pandas import SeriesAgainstTableColumnError
from utilities.sqlalchemy.pandas import SeriesNameNotInTableError
from utilities.sqlalchemy.pandas import SeriesNameSnakeCaseNotInTableError
from utilities.sqlalchemy.pandas import _check_select_for_duplicates
from utilities.sqlalchemy.pandas import _check_series_against_table_column
from utilities.sqlalchemy.pandas import _dataframe_columns_to_snake
from utilities.sqlalchemy.pandas import _parse_series_against_table
from utilities.sqlalchemy.pandas import _rows_to_dataframe
from utilities.sqlalchemy.pandas import _stream_dataframes
from utilities.sqlalchemy.pandas import _table_column_to_dtype
from utilities.sqlalchemy.pandas import _yield_dataframe_rows_as_dicts
from utilities.sqlalchemy.pandas import _yield_insertion_elements
from utilities.sqlalchemy.pandas import insert_dataframe
from utilities.sqlalchemy.pandas import insert_items
from utilities.sqlalchemy.pandas import select_to_dataframe
from utilities.text import snake_case
from utilities.types import NoneType


class TestCheckSelectForDuplicates:
    def test_error(self) -> None:
        table = Table(
            "example",
            MetaData(),
            Column("id", Integer, primary_key=True),
        )
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
    def test_success(self, dtype: Any, column_type: Any) -> None:
        series = Series([], dtype=dtype)
        column = Column("id", column_type)
        _check_series_against_table_column(series, column)

    @mark.parametrize(
        ("dtype", "column_type"),
        [
            param(object, Integer),
            param(datetime64ns, DateTime),
        ],
    )
    def test_error(self, dtype: Any, column_type: Any) -> None:
        series = Series([], dtype=dtype)
        column = Column("id", column_type)
        with raises(SeriesAgainstTableColumnError):
            _check_series_against_table_column(series, column)

    def test_allow_naive_datetimes(self) -> None:
        series = Series([], dtype=datetime64ns)
        column = Column("id", DateTime)
        _check_series_against_table_column(
            series,
            column,
            allow_naive_datetimes=True,
        )


class TestDataFrameColumnsToSnake:
    @given(col_name=text_ascii())
    def test_main(self, col_name: str) -> None:
        df = DataFrame(columns=[col_name])
        snake = _dataframe_columns_to_snake(df)
        assert snake.columns.tolist() == [snake_case(col_name)]


class TestInsertDataFrame:
    @given(
        data=data(),
        engine=sqlite_engines(),
    )
    def test_main(self, data: DataObject, engine: Engine) -> None:
        table = Table(
            "example",
            MetaData(),
            Column("id", Integer, primary_key=True),
        )
        ensure_table_created(table, engine)
        rows = data.draw(table_records_lists(table, max_size=10))
        df = DataFrame(rows, columns=["id"]).astype(int)
        insert_dataframe(df, table, engine)
        with engine.begin() as conn:
            res = conn.execute(select(func.count()).select_from(table)).scalar()
        assert res == len(rows)


class TestInsertItems:
    @given(data=data(), engine=sqlite_engines())
    def test_lists_of_tuples(
        self,
        data: DataObject,
        engine: Engine,
    ) -> None:
        table = Table(
            "example",
            MetaData(),
            Column("id", Integer, primary_key=True),
        )
        ensure_table_created(table, engine)
        rows = data.draw(table_records_lists(table, max_size=10))
        insert_items([(rows, table)], engine)
        with engine.begin() as conn:
            res = conn.execute(select(func.count()).select_from(table)).scalar()
        assert res == len(rows)

    @given(data=data(), engine=sqlite_engines())
    def test_dataframe_with_table(
        self,
        data: DataObject,
        engine: Engine,
    ) -> None:
        table = Table(
            "example",
            MetaData(),
            Column("id", Integer, primary_key=True),
        )
        ensure_table_created(table, engine)
        rows = data.draw(table_records_lists(table, max_size=10))
        df = DataFrame(rows, columns=["id"]).astype(int)
        insert_items([(df, table)], engine)
        with engine.begin() as conn:
            res = conn.execute(select(func.count()).select_from(table)).scalar()
        assert res == len(rows)

    @given(data=data(), engine=sqlite_engines())
    def test_model(self, data: DataObject, engine: Engine) -> None:
        class Example(cast(Any, declarative_base())):
            __tablename__ = "example"

            id_ = Column(Integer, primary_key=True)

        ensure_table_created(Example, engine)
        rows = data.draw(table_records_lists(get_table(Example), max_size=10))
        items = [Example(id_=id_) for id_, in rows]
        insert_items(items, engine)
        with engine.begin() as conn:
            res = conn.execute(
                select(func.count()).select_from(get_table(Example)),
            ).scalar()
        assert res == len(rows)

    @given(engine=sqlite_engines())
    def test_type_error(self, engine: Engine) -> None:
        table = Table(
            "example",
            MetaData(),
            Column("id", Integer, primary_key=True),
        )
        items = [(None, table)]
        with raises(TypeError, match="Invalid type: first="):
            insert_items(items, engine)


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
        self,
        series_name: str,
        table_column_name: str,
        snake: bool,
    ) -> None:
        series = Series([], dtype=int, name=series_name)
        table = Table(
            "example",
            MetaData(),
            Column(table_column_name, Integer, primary_key=True),
        )
        key, _ = _parse_series_against_table(
            series,
            table,
            snake=snake,
        )
        assert key == table_column_name

    @mark.parametrize(
        ("snake", "error"),
        [
            param(True, SeriesNameSnakeCaseNotInTableError),
            param(False, SeriesNameNotInTableError),
        ],
    )
    def test_error(
        self,
        snake: bool,
        error: type[Exception],
    ) -> None:
        series = Series([], dtype=int, name="name")
        table = Table(
            "example",
            MetaData(),
            Column("id", Integer, primary_key=True),
        )
        with raises(error):
            _ = _parse_series_against_table(
                series,
                table,
                snake=snake,
            )


class TestRowsToDataFrame:
    @given(data=data(), engine=sqlite_engines())
    @mark.parametrize(
        ("col_name", "snake"),
        [param("id", False), param("Id", True)],
    )
    def test_main(
        self,
        data: DataObject,
        col_name: str,
        engine: Engine,
        snake: bool,
    ) -> None:
        table = Table(
            "example",
            MetaData(),
            Column(col_name, Integer, primary_key=True),
        )
        ensure_table_created(table, engine)
        values = data.draw(table_records_lists(table, min_size=1, max_size=10))
        with engine.begin() as conn:
            _ = conn.execute(insert(table).values(values))
        with engine.begin() as conn:
            rows = conn.execute(sel := select(table)).all()
        df = _rows_to_dataframe(sel, rows, snake=snake)
        assert len(df) == len(values)
        assert dict(df.dtypes) == {"id": Int64}


class TestSelectToDataFrame:
    @given(
        data=data(),
        engine=sqlite_engines(),
        stream=integers(1, 10) | none(),
    )
    def test_main(
        self,
        data: DataObject,
        engine: Engine,
        stream: Optional[int],
    ) -> None:
        table = Table(
            "example",
            MetaData(),
            Column("id", Integer, primary_key=True),
        )
        ensure_table_created(table, engine)
        rows = data.draw(table_records_lists(table, min_size=1, max_size=10))
        insert_items([(rows, table)], engine)
        result = select_to_dataframe(select(table), engine, stream=stream)
        if stream is None:
            assert isinstance(result, DataFrame)
            assert len(cast(Sized, result)) == len(rows)
            assert dict(cast(Any, result).dtypes) == {"id": Int64}
        else:
            assert not isinstance(result, DataFrame)
            for df in result:
                assert 1 <= len(df) <= stream
                assert dict(df.dtypes) == {"id": Int64}


class TestStreamDataFrames:
    @given(data=data(), engine=sqlite_engines(), stream=integers(1, 10))
    def test_main(self, data: DataObject, engine: Engine, stream: int) -> None:
        table = Table(
            "example",
            MetaData(),
            Column("id", Integer, primary_key=True),
        )
        ensure_table_created(table, engine)
        rows = data.draw(table_records_lists(table, min_size=1, max_size=10))
        insert_items([(rows, table)], engine)
        for df in _stream_dataframes(select(table), engine, stream):
            assert 1 <= len(df) <= stream
            assert dict(df.dtypes) == {"id": Int64}

    @given(engine=sqlite_engines())
    def test_non_positive_stream(self, engine: Engine) -> None:
        table = Table(
            "example",
            MetaData(),
            Column("id", Integer, primary_key=True),
        )
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
    def test_main(self, column: Any, expected: Any) -> None:
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
            param(int64s(), int, Integer, int),
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
            "example",
            MetaData(),
            Column("id", table_dtype, primary_key=True),
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
            param(int64s(), int, int),
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
        expected: Union[type, tuple[type, ...]],
    ) -> None:
        values = data.draw(lists(elements, max_size=10))
        series = Series(values, dtype=dtype)
        for el in _yield_insertion_elements(series):
            assert isinstance(el, expected)

    @given(date=dates_pd())
    def test_dates_with_time_components_error(self, date: dt.date) -> None:
        series = Series(
            [dt.datetime.combine(date, dt.time(12))],
            dtype=datetime64ns,
        )
        with raises(DatesWithTimeComponentsError):
            _ = list(_yield_insertion_elements(series))

    def test_type_error(self) -> None:
        series = Series(dtype=object)
        with raises(TypeError, match="Invalid dtype"):
            _ = list(_yield_insertion_elements(series))
