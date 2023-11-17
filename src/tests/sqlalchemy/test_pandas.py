from __future__ import annotations

import datetime as dt
from collections.abc import Callable
from operator import eq
from typing import Any

from hypothesis import assume, given
from hypothesis.strategies import (
    DataObject,
    SearchStrategy,
    booleans,
    data,
    floats,
    integers,
    lists,
    none,
    sets,
)
from numpy import int64, isnan
from pandas import DataFrame
from pandas.testing import assert_frame_equal
from pytest import mark, param, raises
from sqlalchemy import (
    DECIMAL,
    Boolean,
    Column,
    Date,
    DateTime,
    Engine,
    Float,
    Integer,
    LargeBinary,
    MetaData,
    String,
    Table,
    select,
)

from utilities.datetime import date_to_datetime
from utilities.hypothesis import dates_pd, datetimes_pd, sqlite_engines, text_ascii
from utilities.numpy import datetime64ns
from utilities.pandas import Int64, boolean, check_dataframe, datetime64nsutc, string
from utilities.sqlalchemy import (
    ColumnToPandasDTypeError,
    NonPositiveStreamError,
    PandasDataFrameYieldsNoRowsError,
    ensure_tables_created,
    insert_items,
    insert_pandas_dataframe,
    select_to_pandas_dataframe,
)
from utilities.sqlalchemy.pandas import (
    _dataframe_columns_to_snake,
    _rows_to_dataframe,
    _stream_dataframes,
    _table_column_to_dtype,
)
from utilities.text import snake_case


class TestDataFrameColumnsToSnake:
    @given(col_name=text_ascii())
    def test_main(self, *, col_name: str) -> None:
        df = DataFrame(columns=[col_name])
        snake = _dataframe_columns_to_snake(df)
        assert snake.columns.tolist() == [snake_case(col_name)]


def _check_datetime(x: dt.datetime | None, y: dt.datetime | None, /) -> bool:
    return (x == y) or (
        isinstance(x, dt.datetime)
        and isinstance(y, dt.datetime)
        and (x.replace(tzinfo=None) == y.replace(tzinfo=None))
    )


def _check_float(x: float | None, y: float | None, /) -> bool:
    return (
        (x == y)
        or ((x is None) and isinstance(y, float) and isnan(y))
        or (isinstance(x, float) and isnan(x) and (y is None))
    )


class TestInsertPandasDataFrame:
    @given(data=data(), engine=sqlite_engines())
    @mark.parametrize(
        ("strategy", "pd_dtype", "col_type", "check"),
        [
            param(booleans(), bool, Boolean, eq),
            param(booleans() | none(), boolean, Boolean, eq),
            param(dates_pd() | none(), datetime64ns, Date, eq),
            param(
                datetimes_pd().map(lambda x: x.replace(tzinfo=None)) | none(),
                datetime64ns,
                DateTime,
                _check_datetime,
            ),
            param(
                datetimes_pd() | none(),
                datetime64nsutc,
                DateTime(timezone=True),
                _check_datetime,
            ),
            param(floats(), float, Float, _check_float),
            param(integers(-10, 10), int, Integer, eq),
            param(integers(-10, 10), int64, Integer, eq),
            param(integers(-10, 10) | none(), Int64, Integer, eq),
            param(text_ascii() | none(), string, String, eq),
        ],
    )
    def test_main(
        self,
        *,
        data: DataObject,
        engine: Engine,
        strategy: SearchStrategy[Any],
        pd_dtype: Any,
        col_type: Any,
        check: Callable[[Any, Any], bool],
    ) -> None:
        values = data.draw(lists(strategy))
        df = DataFrame(values, columns=["value"], dtype=pd_dtype)
        table = Table(
            "example",
            MetaData(),
            Column("id", Integer, primary_key=True),
            Column("value", col_type),
        )
        ensure_tables_created(engine, table)
        insert_pandas_dataframe(engine, df, table)
        sel = select(table.c["value"])
        with engine.begin() as conn:
            res = conn.execute(sel).scalars().all()
        for r, v in zip(res, values, strict=True):
            assert check(r, v)

    @given(
        engine=sqlite_engines(),
        date=dates_pd(),
        datetime=datetimes_pd().map(lambda x: x.replace(tzinfo=None)),
    )
    def test_mixed_date_and_datetimes(
        self, *, engine: Engine, date: dt.date, datetime: dt.datetime
    ) -> None:
        _ = assume(
            (datetime.hour != 0) or (datetime.minute != 0) or (datetime.second != 0)
        )
        df = DataFrame([date, datetime], columns=["value"], dtype=datetime64ns)
        table = Table(
            "example",
            MetaData(),
            Column("id", Integer, primary_key=True),
            Column("value", DateTime),
        )
        ensure_tables_created(engine, table)
        insert_pandas_dataframe(engine, df, table)
        sel = select(table.c["value"])
        with engine.begin() as conn:
            res = conn.execute(sel).scalars().all()
        expected = [date_to_datetime(date, tzinfo=None), datetime]
        assert res == expected

    @given(engine=sqlite_engines(), values=lists(booleans() | none(), min_size=1))
    def test_pandas_data_frame_yields_no_rows_error(
        self, *, engine: Engine, values: list[bool | None]
    ) -> None:
        table = Table(
            "example",
            MetaData(),
            Column("id", Integer, primary_key=True),
            Column("value", Boolean),
        )
        df = DataFrame(values, columns=["other"], dtype=boolean)
        with raises(PandasDataFrameYieldsNoRowsError):
            insert_pandas_dataframe(engine, df, table)


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


class TestSelectToPandasDataFrame:
    @given(data=data(), engine=sqlite_engines())
    @mark.parametrize(
        ("strategy", "pd_dtype_in", "pd_dtype_out", "col_type"),
        [
            param(booleans(), bool, boolean, Boolean),
            param(booleans() | none(), boolean, boolean, Boolean),
            param(dates_pd() | none(), datetime64ns, datetime64ns, Date),
            param(
                datetimes_pd().map(lambda x: x.replace(tzinfo=None)) | none(),
                datetime64ns,
                datetime64ns,
                DateTime,
            ),
            param(
                datetimes_pd() | none(),
                datetime64nsutc,
                datetime64nsutc,
                DateTime(timezone=True),
            ),
            param(floats(), float, float, Float),
            param(integers(-10, 10), int, Int64, Integer),
            param(integers(-10, 10), int64, Int64, Integer),
            param(integers(-10, 10) | none(), Int64, Int64, Integer),
            param(text_ascii() | none(), string, string, String),
        ],
    )
    def test_main(
        self,
        *,
        data: DataObject,
        engine: Engine,
        pd_dtype_in: Any,
        pd_dtype_out: Any,
        col_type: Any,
        strategy: SearchStrategy[Any],
    ) -> None:
        values = data.draw(lists(strategy))
        df = DataFrame(values, columns=["value"], dtype=pd_dtype_in)
        table = Table(
            "example",
            MetaData(),
            Column("id", Integer, primary_key=True),
            Column("value", col_type),
        )
        ensure_tables_created(engine, table)
        insert_pandas_dataframe(engine, df, table)
        sel = select(table.c["value"])
        result = select_to_pandas_dataframe(sel, engine)
        expected = DataFrame(values, columns=["value"], dtype=pd_dtype_out)
        assert_frame_equal(result, expected)

    @given(
        engine=sqlite_engines(),
        values=lists(booleans() | none()),
        stream=integers(1, 10),
    )
    def test_stream(
        self, *, engine: Engine, values: list[bool | None], stream: int
    ) -> None:
        df = DataFrame(values, columns=["value"], dtype=boolean)
        table = Table(
            "example",
            MetaData(),
            Column("id", Integer, primary_key=True),
            Column("value", Boolean),
        )
        ensure_tables_created(engine, table)
        insert_pandas_dataframe(engine, df, table)
        sel = select(table.c["value"])
        dfs = select_to_pandas_dataframe(sel, engine, stream=stream)
        for df in dfs:
            check_dataframe(
                df, dtypes={"value": boolean}, min_length=1, max_length=stream
            )

    @given(engine=sqlite_engines(), values=lists(booleans() | none()))
    def test_snake(self, *, engine: Engine, values: list[bool | None]) -> None:
        df = DataFrame(values, columns=["Value"], dtype=boolean)
        table = Table(
            "example",
            MetaData(),
            Column("id", Integer, primary_key=True),
            Column("Value", Boolean),
        )
        ensure_tables_created(engine, table)
        insert_pandas_dataframe(engine, df, table)
        sel = select(table.c["Value"])
        result = select_to_pandas_dataframe(sel, engine, snake=True)
        expected = DataFrame(values, columns=["value"], dtype=boolean)
        assert_frame_equal(result, expected)

    @given(engine=sqlite_engines())
    def test_column_to_pandas_dtype_error(self, *, engine: Engine) -> None:
        table = Table(
            "example",
            MetaData(),
            Column("id", Integer, primary_key=True),
            Column("value", LargeBinary),
        )
        ensure_tables_created(engine, table)
        sel = select(table.c["value"])
        with raises(ColumnToPandasDTypeError):
            _ = select_to_pandas_dataframe(sel, engine)


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
            param(Column(DECIMAL), float),
            param(Column(Float), float),
            param(Column(Integer), Int64),
            param(Column(String), string),
        ],
    )
    def test_main(self, *, column: Any, expected: Any) -> None:
        assert _table_column_to_dtype(column) == expected
