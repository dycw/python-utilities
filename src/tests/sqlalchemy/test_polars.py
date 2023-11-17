from __future__ import annotations

import datetime as dt
from collections.abc import Callable
from math import isnan
from operator import eq
from typing import Any

import polars as pl
import sqlalchemy
from hypothesis import given
from hypothesis.strategies import (
    DataObject,
    SearchStrategy,
    booleans,
    data,
    dates,
    datetimes,
    floats,
    integers,
    just,
    lists,
    none,
)
from polars import DataFrame, Datetime, Float64, Int64, PolarsDataType, Utf8
from polars.testing import assert_frame_equal
from pytest import mark, param, raises
from sqlalchemy import (
    Column,
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

from utilities.datetime import UTC
from utilities.hypothesis import sqlite_engines, text_ascii
from utilities.polars import check_dataframe
from utilities.sqlalchemy import (
    ColumnToPolarsExprError,
    PolarsDataFrameYieldsNoRowsError,
    ensure_tables_created,
    insert_polars_dataframe,
    select_to_polars_dataframe,
)


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


class TestInsertPolarsDataFrame:
    @given(data=data(), engine=sqlite_engines())
    @mark.parametrize(
        ("strategy", "pl_dtype", "col_type", "check"),
        [
            param(booleans() | none(), pl.Boolean, sqlalchemy.Boolean, eq),
            param(dates() | none(), pl.Date, sqlalchemy.Date, eq),
            param(datetimes() | none(), Datetime, DateTime, eq),
            param(
                datetimes(timezones=just(UTC)) | none(),
                Datetime(time_zone=UTC),
                DateTime(timezone=True),
                _check_datetime,
            ),
            param(floats(), Float64, Float, _check_float),
            param(integers(-10, 10) | none(), Int64, Integer, eq),
            param(text_ascii() | none(), Utf8, String, eq),
        ],
    )
    def test_main(
        self,
        *,
        data: DataObject,
        engine: Engine,
        strategy: SearchStrategy[Any],
        pl_dtype: PolarsDataType,
        col_type: Any,
        check: Callable[[Any, Any], bool],
    ) -> None:
        values = data.draw(lists(strategy))
        df = DataFrame({"value": values}, schema={"value": pl_dtype})
        table = Table(
            "example",
            MetaData(),
            Column("id", Integer, primary_key=True),
            Column("value", col_type),
        )
        ensure_tables_created(engine, table)
        insert_polars_dataframe(engine, df, table)
        sel = select(table.c["value"])
        with engine.begin() as conn:
            res = conn.execute(sel).scalars().all()
        for r, v in zip(res, values, strict=True):
            assert check(r, v)

    @given(engine=sqlite_engines(), values=lists(booleans() | none()))
    @mark.parametrize("sr_name", [param("Value"), param("value")])
    def test_snake(
        self, *, engine: Engine, values: list[bool | None], sr_name: str
    ) -> None:
        df = DataFrame({sr_name: values}, schema={sr_name: pl.Boolean})
        table = Table(
            "example",
            MetaData(),
            Column("Id", Integer, primary_key=True),
            Column("Value", sqlalchemy.Boolean),
        )
        ensure_tables_created(engine, table)
        insert_polars_dataframe(engine, df, table, snake=True)
        sel = select(table.c["Value"])
        with engine.begin() as conn:
            res = conn.execute(sel).scalars().all()
        assert res == values

    @given(engine=sqlite_engines(), values=lists(booleans() | none(), min_size=1))
    def test_polars_data_frame_yields_no_rows_error(
        self, *, engine: Engine, values: list[bool | None]
    ) -> None:
        table = Table(
            "example",
            MetaData(),
            Column("id", Integer, primary_key=True),
            Column("value", sqlalchemy.Boolean),
        )
        df = DataFrame({"other": values}, schema={"other": pl.Boolean})
        with raises(PolarsDataFrameYieldsNoRowsError):
            insert_polars_dataframe(engine, df, table)


class TestSelectToPolarsDataFrame:
    @given(data=data(), engine=sqlite_engines())
    @mark.parametrize(
        ("strategy", "pl_dtype", "col_type"),
        [
            param(booleans() | none(), pl.Boolean, sqlalchemy.Boolean),
            param(dates() | none(), pl.Date, sqlalchemy.Date),
            param(datetimes() | none(), Datetime, DateTime),
            param(
                datetimes(timezones=just(UTC)) | none(),
                Datetime(time_zone=UTC),
                DateTime(timezone=True),
            ),
            param(floats(), Float64, Float),
            param(integers(-10, 10) | none(), Int64, Integer),
            param(text_ascii() | none(), Utf8, String),
        ],
    )
    def test_main(
        self,
        *,
        data: DataObject,
        engine: Engine,
        strategy: SearchStrategy[Any],
        pl_dtype: PolarsDataType,
        col_type: Any,
    ) -> None:
        values = data.draw(lists(strategy, min_size=1))
        df = DataFrame({"value": values}, schema={"value": pl_dtype})
        table = Table(
            "example",
            MetaData(),
            Column("id", Integer, primary_key=True),
            Column("value", col_type),
        )
        ensure_tables_created(engine, table)
        insert_polars_dataframe(engine, df, table)
        sel = select(table.c["value"])
        result = select_to_polars_dataframe(sel, engine)
        assert_frame_equal(result, df)

    @given(
        engine=sqlite_engines(),
        values=lists(booleans() | none()),
        batch_size=integers(1, 10),
    )
    def test_iter_batches(
        self, *, engine: Engine, values: list[bool | None], batch_size: int
    ) -> None:
        df = DataFrame({"value": values}, schema={"value": pl.Boolean})
        table = Table(
            "example",
            MetaData(),
            Column("id", Integer, primary_key=True),
            Column("value", sqlalchemy.Boolean),
        )
        ensure_tables_created(engine, table)
        insert_polars_dataframe(engine, df, table)
        sel = select(table.c["value"])
        dfs = select_to_polars_dataframe(
            sel, engine, iter_batches=True, batch_size=batch_size
        )
        for df in dfs:
            check_dataframe(
                df, min_height=1, max_height=batch_size, schema={"value": pl.Boolean}
            )

    @given(engine=sqlite_engines(), values=lists(booleans() | none()))
    def test_snake(self, *, engine: Engine, values: list[bool | None]) -> None:
        df = DataFrame({"Value": values}, schema={"Value": pl.Boolean})
        table = Table(
            "example",
            MetaData(),
            Column("Id", Integer, primary_key=True),
            Column("Value", sqlalchemy.Boolean),
        )
        ensure_tables_created(engine, table)
        insert_polars_dataframe(engine, df, table)
        sel = select(table.c["Value"])
        res = select_to_polars_dataframe(sel, engine, snake=True)
        expected = DataFrame({"value": values}, schema={"value": pl.Boolean})
        assert_frame_equal(res, expected)

    @given(engine=sqlite_engines())
    def test_column_to_polars_expr_error(self, *, engine: Engine) -> None:
        table = Table(
            "example",
            MetaData(),
            Column("id", Integer, primary_key=True),
            Column("value", LargeBinary),
        )
        ensure_tables_created(engine, table)
        sel = select(table.c["value"])
        with raises(ColumnToPolarsExprError):
            _ = select_to_polars_dataframe(sel, engine)
