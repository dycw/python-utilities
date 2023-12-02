from __future__ import annotations

from collections.abc import Callable
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
from sqlalchemy.exc import DuplicateColumnError

from utilities._sqlalchemy.polars import (
    _insert_dataframe_map_df_column_to_table_column_and_type,
    _insert_dataframe_map_df_column_to_table_schema,
    _insert_dataframe_map_df_schema_to_table,
    _InsertDataFrameMapDFColumnToTableColumnAndTypeError,
    _InsertDataFrameMapDFColumnToTableSchemaError,
    _select_to_dataframe_apply_snake,
    _select_to_dataframe_check_duplicates,
    _select_to_dataframe_map_select_to_df_schema,
    _SelectToDataFrameMapTableColumnToDTypeError,
)
from utilities.datetime import UTC, is_equal_mod_tz
from utilities.hypothesis import sqlite_engines, text_ascii
from utilities.math import is_equal
from utilities.polars import check_polars_dataframe
from utilities.sqlalchemy import (
    InsertDataFrameError,
    _select_to_dataframe_map_table_column_to_dtype,
    ensure_tables_created,
    insert_dataframe,
    select_to_dataframe,
)


class TestInsertDataFrame:
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
                is_equal_mod_tz,
            ),
            param(floats(allow_nan=False) | none(), Float64, Float, is_equal),
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
        insert_dataframe(engine, df, table)
        sel = select(table.c["value"])
        with engine.begin() as conn:
            res = conn.execute(sel).scalars().all()
        for r, v in zip(res, values, strict=True):
            assert ((r is None) == (v is None)) or check(r, v)

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
        insert_dataframe(engine, df, table, snake=True)
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
        with raises(InsertDataFrameError):
            insert_dataframe(engine, df, table)


class TestInsertDataFrameMapDFColumnToTableColumnAndType:
    def test_main(self) -> None:
        schema = {"a": int, "b": float, "c": str}
        result = _insert_dataframe_map_df_column_to_table_column_and_type("b", schema)
        expected = ("b", float)
        assert result == expected

    @mark.parametrize("sr_name", [param("b"), param("B")])
    def test_snake(self, *, sr_name: str) -> None:
        schema = {"A": int, "B": float, "C": str}
        result = _insert_dataframe_map_df_column_to_table_column_and_type(
            sr_name, schema, snake=True
        )
        expected = ("B", float)
        assert result == expected

    @mark.parametrize("snake", [param(True), param(False)])
    def test_error_empty(self, *, snake: bool) -> None:
        schema = {"a": int, "b": float, "c": str}
        with raises(_InsertDataFrameMapDFColumnToTableColumnAndTypeError):
            _ = _insert_dataframe_map_df_column_to_table_column_and_type(
                "value", schema, snake=snake
            )

    def test_error_non_unique(self) -> None:
        schema = {"a": int, "b": float, "B": float, "c": str}
        with raises(_InsertDataFrameMapDFColumnToTableColumnAndTypeError):
            _ = _insert_dataframe_map_df_column_to_table_column_and_type(
                "b", schema, snake=True
            )


class TestInsertDataFrameMapDFColumnToTableSchema:
    def test_main(self) -> None:
        table_schema = {"a": int, "b": float, "c": str}
        result = _insert_dataframe_map_df_column_to_table_schema(
            "b", Float64, table_schema
        )
        assert result == "b"

    def test_error(self) -> None:
        table_schema = {"a": int, "b": float, "c": str}
        with raises(_InsertDataFrameMapDFColumnToTableSchemaError):
            _ = _insert_dataframe_map_df_column_to_table_schema(
                "b", Int64, table_schema
            )


class TestInsertDataFrameMapDFSchemaToTable:
    def test_default(self) -> None:
        df_schema = {"a": Int64, "b": Float64}
        table = Table(
            "example",
            MetaData(),
            Column("id", Integer, primary_key=True),
            Column("a", Integer),
            Column("b", Float),
        )
        result = _insert_dataframe_map_df_schema_to_table(df_schema, table)
        expected = {"a": "a", "b": "b"}
        assert result == expected

    def test_snake(self) -> None:
        df_schema = {"a": Int64, "b": Float64}
        table = Table(
            "example",
            MetaData(),
            Column("Id", Integer, primary_key=True),
            Column("A", Integer),
            Column("B", Float),
        )
        result = _insert_dataframe_map_df_schema_to_table(df_schema, table, snake=True)
        expected = {"a": "A", "b": "B"}
        assert result == expected

    def test_df_schema_has_extra_columns(self) -> None:
        df_schema = {"a": Int64, "b": Float64, "c": Utf8}
        table = Table(
            "example",
            MetaData(),
            Column("id", Integer, primary_key=True),
            Column("a", Integer),
            Column("b", Float),
        )
        result = _insert_dataframe_map_df_schema_to_table(df_schema, table)
        expected = {"a": "a", "b": "b"}
        assert result == expected

    def test_table_has_extra_columns(self) -> None:
        df_schema = {"a": Int64, "b": Float64}
        table = Table(
            "example",
            MetaData(),
            Column("id", Integer, primary_key=True),
            Column("a", Integer),
            Column("b", Float),
            Column("c", String),
        )
        result = _insert_dataframe_map_df_schema_to_table(df_schema, table)
        expected = {"a": "a", "b": "b"}
        assert result == expected


class TestSelectToDataFrame:
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
            param(floats(allow_nan=False) | none(), Float64, Float),
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
        insert_dataframe(engine, df, table)
        sel = select(table.c["value"])
        result = select_to_dataframe(sel, engine)
        assert_frame_equal(result, df)

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
        insert_dataframe(engine, df, table)
        sel = select(table.c["Value"])
        res = select_to_dataframe(sel, engine, snake=True)
        expected = DataFrame({"value": values}, schema={"value": pl.Boolean})
        assert_frame_equal(res, expected)

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
        insert_dataframe(engine, df, table)
        sel = select(table.c["value"])
        with engine.begin() as conn:
            dfs = list(
                select_to_dataframe(sel, conn, iter_batches=True, batch_size=batch_size)
            )
        for df in dfs:
            check_polars_dataframe(
                df, min_height=1, max_height=batch_size, schema={"value": pl.Boolean}
            )


class TestSelectToDataFrameApplySnake:
    def test_main(self) -> None:
        table = Table(
            "example",
            MetaData(),
            Column("Id", Integer, primary_key=True),
            Column("Value", sqlalchemy.Boolean),
        )
        sel = select(table)
        res = _select_to_dataframe_apply_snake(sel)
        expected = ["id", "value"]
        for col, exp in zip(res.c, expected, strict=True):
            assert col.name == exp


class TestSelectToDataFrameCheckDuplicates:
    def test_error(self) -> None:
        table = Table("example", MetaData(), Column("id", Integer, primary_key=True))
        sel = select(table.c.id, table.c.id)
        with raises(DuplicateColumnError):
            _select_to_dataframe_check_duplicates(sel.c)


class TestSelectToDataFrameMapSelectToDFSchema:
    def test_main(self) -> None:
        table = Table("example", MetaData(), Column("id", Integer, primary_key=True))
        sel = select(table.c.id)
        schema = _select_to_dataframe_map_select_to_df_schema(sel)
        expected = {"id": Int64}
        assert schema == expected


class TestSelectToDataFrameMapTableColumnToDType:
    def test_error(self) -> None:
        column = Column("value", LargeBinary)
        with raises(_SelectToDataFrameMapTableColumnToDTypeError):
            _ = _select_to_dataframe_map_table_column_to_dtype(column)
