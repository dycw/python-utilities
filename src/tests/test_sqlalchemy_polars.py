from __future__ import annotations

from operator import eq
from pathlib import Path
from typing import TYPE_CHECKING, Any, ClassVar, TypeVar

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
    sampled_from,
    sets,
)
from more_itertools import classify_unique
from polars import (
    Binary,
    DataFrame,
    Datetime,
    Decimal,
    Duration,
    Float64,
    Int32,
    Int64,
    Utf8,
)
from polars.testing import assert_frame_equal
from pytest import mark, param, raises
from sqlalchemy import (
    BIGINT,
    BINARY,
    BOOLEAN,
    CHAR,
    CLOB,
    DATE,
    DATETIME,
    DECIMAL,
    DOUBLE,
    DOUBLE_PRECISION,
    FLOAT,
    INT,
    INTEGER,
    NCHAR,
    NUMERIC,
    NVARCHAR,
    REAL,
    SMALLINT,
    TEXT,
    TIME,
    TIMESTAMP,
    UUID,
    VARBINARY,
    VARCHAR,
    BigInteger,
    Column,
    DateTime,
    Double,
    Engine,
    Float,
    Integer,
    Interval,
    LargeBinary,
    MetaData,
    Numeric,
    Select,
    SmallInteger,
    String,
    Table,
    Text,
    Unicode,
    UnicodeText,
    Uuid,
    select,
)
from sqlalchemy.exc import DuplicateColumnError

from utilities.datetime import is_equal_mod_tz
from utilities.hypothesis import (
    aiosqlite_engines,
    assume_does_not_raise,
    sqlite_engines,
    text_ascii,
)
from utilities.math import is_equal
from utilities.polars import check_polars_dataframe
from utilities.sqlalchemy import ensure_tables_created, ensure_tables_created_async
from utilities.sqlalchemy_polars import (
    InsertDataFrameError,
    _insert_dataframe_map_df_column_to_table_column_and_type,
    _insert_dataframe_map_df_column_to_table_schema,
    _insert_dataframe_map_df_schema_to_table,
    _InsertDataFrameMapDFColumnToTableColumnAndTypeError,
    _InsertDataFrameMapDFColumnToTableSchemaError,
    _select_to_dataframe_apply_snake,
    _select_to_dataframe_check_duplicates,
    _select_to_dataframe_map_select_to_df_schema,
    _select_to_dataframe_map_table_column_type_to_dtype,
    _select_to_dataframe_yield_selects_with_in_clauses,
    insert_dataframe,
    insert_dataframe_async,
    select_to_dataframe,
    select_to_dataframe_async,
)
from utilities.zoneinfo import UTC

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

    from _pytest.mark import ParameterSet
    from polars._typing import PolarsDataType
    from polars.datatypes import DataTypeClass


_T = TypeVar("_T")


class TestInsertDataFrame:
    cases: ClassVar[list[ParameterSet]] = [
        param(booleans() | none(), pl.Boolean, sqlalchemy.Boolean, eq),
        param(dates() | none(), pl.Date, sqlalchemy.Date, eq),
        param(datetimes() | none(), Datetime, DateTime, eq),
        param(
            datetimes(timezones=just(UTC)) | none(),
            Datetime(time_zone="UTC"),
            DateTime(timezone=True),
            is_equal_mod_tz,
        ),
        param(floats(allow_nan=False) | none(), Float64, Float, is_equal),
        param(integers(-10, 10) | none(), Int32, Integer, eq),
        param(integers(-10, 10) | none(), Int64, Integer, eq),
        param(text_ascii() | none(), Utf8, String, eq),
    ]

    @given(data=data(), engine=sqlite_engines())
    @mark.parametrize(("strategy", "pl_dtype", "col_type", "check"), cases)
    def test_sync(
        self,
        *,
        data: DataObject,
        engine: Engine,
        strategy: SearchStrategy[Any],
        pl_dtype: PolarsDataType,
        col_type: Any,
        check: Callable[[Any, Any], bool],
    ) -> None:
        values, df, table, sel = self._prepare_main_test(
            data, strategy, pl_dtype, col_type
        )
        insert_dataframe(df, table, engine)
        with engine.begin() as conn:
            res = conn.execute(sel).scalars().all()
        self._assert_results(res, values, check)

    @given(engine=sqlite_engines(), values=lists(booleans() | none(), max_size=100))
    @mark.parametrize("sr_name", [param("Value"), param("value")])
    def test_sync_snake(
        self, *, engine: Engine, values: list[bool | None], sr_name: str
    ) -> None:
        df, table, sel = self._prepare_snake_test(sr_name, values)
        insert_dataframe(df, table, engine, snake=True)
        with engine.begin() as conn:
            res = conn.execute(sel).scalars().all()
        assert res == values

    @given(
        values=lists(booleans() | none(), min_size=1, max_size=100),
        engine=sqlite_engines(),
    )
    def test_sync_error(self, *, values: list[bool | None], engine: Engine) -> None:
        df, table = self._prepare_empty_test(values)
        with raises(
            InsertDataFrameError,
            match="Non-empty DataFrame must resolve to at least 1 item",
        ):
            insert_dataframe(df, table, engine)

    @given(data=data())
    @mark.parametrize(("strategy", "pl_dtype", "col_type", "check"), cases)
    async def test_async(
        self,
        *,
        data: DataObject,
        strategy: SearchStrategy[Any],
        pl_dtype: PolarsDataType,
        col_type: Any,
        check: Callable[[Any, Any], bool],
    ) -> None:
        values, df, table, sel = self._prepare_main_test(
            data, strategy, pl_dtype, col_type
        )
        engine = await aiosqlite_engines(data)
        await insert_dataframe_async(df, table, engine)
        async with engine.begin() as conn:
            res = (await conn.execute(sel)).scalars().all()
        self._assert_results(res, values, check)

    @given(data=data(), values=lists(booleans() | none(), max_size=100))
    @mark.parametrize("sr_name", [param("Value"), param("value")])
    async def test_async_snake(
        self, *, data: DataObject, values: list[bool | None], sr_name: str
    ) -> None:
        df, table, sel = self._prepare_snake_test(sr_name, values)
        engine = await aiosqlite_engines(data)
        await insert_dataframe_async(df, table, engine, snake=True)
        async with engine.begin() as conn:
            res = (await conn.execute(sel)).scalars().all()
        assert res == values

    @given(data=data(), values=lists(booleans() | none(), min_size=1, max_size=100))
    async def test_async_error(
        self, *, data: DataObject, values: list[bool | None]
    ) -> None:
        df, table = self._prepare_empty_test(values)
        engine = await aiosqlite_engines(data)
        with raises(
            InsertDataFrameError,
            match="Non-empty DataFrame must resolve to at least 1 item",
        ):
            _ = await insert_dataframe_async(df, table, engine)

    def _prepare_main_test(
        self,
        data: DataObject,
        strategy: SearchStrategy[_T],
        pl_dtype: PolarsDataType,
        col_type: Any,
        /,
    ) -> tuple[Sequence[_T], DataFrame, Table, Select[Any]]:
        values = data.draw(lists(strategy, max_size=100))
        df = DataFrame({"value": values}, schema={"value": pl_dtype})
        table = self._make_table(col_type)
        sel = select(table.c["value"])
        return values, df, table, sel

    def _make_table(self, type_: Any, /, *, title: bool = False) -> Table:
        return Table(
            "example",
            MetaData(),
            Column("Id" if title else "id", Integer, primary_key=True),
            Column("Value" if title else "value", type_),
        )

    def _assert_results(
        self,
        results: Sequence[Any],
        values: Sequence[Any],
        check: Callable[[Any, Any], bool],
        /,
    ) -> None:
        for r, v in zip(results, values, strict=True):
            assert ((r is None) == (v is None)) or check(r, v)

    def _prepare_snake_test(
        self, sr_name: str, values: Sequence[bool | None], /
    ) -> tuple[DataFrame, Table, Select[Any]]:
        df = DataFrame({sr_name: values}, schema={sr_name: pl.Boolean})
        table = self._make_table(sqlalchemy.Boolean, title=True)
        sel = select(table.c["Value"])
        return df, table, sel

    def _prepare_empty_test(
        self, values: Sequence[bool | None], /
    ) -> tuple[DataFrame, Table]:
        df = DataFrame({"other": values}, schema={"other": pl.Boolean})
        table = self._make_table(sqlalchemy.Boolean)
        return df, table


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


@mark.only
class TestSelectToDataFrame:
    cases: ClassVar[list[ParameterSet]] = [
        param(booleans() | none(), pl.Boolean, sqlalchemy.Boolean),
        param(dates() | none(), pl.Date, sqlalchemy.Date),
        param(datetimes() | none(), Datetime, DateTime),
        param(
            datetimes(timezones=just(UTC)) | none(),
            Datetime(time_zone="UTC"),
            DateTime(timezone=True),
        ),
        param(floats(allow_nan=False) | none(), Float64, Float),
        param(integers(-10, 10) | none(), Int64, Integer),
        param(text_ascii() | none(), Utf8, String),
    ]

    @given(data=data(), engine=sqlite_engines())
    @mark.parametrize(("strategy", "pl_dtype", "col_type"), cases)
    def test_sync(
        self,
        *,
        data: DataObject,
        engine: Engine,
        strategy: SearchStrategy[Any],
        pl_dtype: PolarsDataType,
        col_type: Any,
    ) -> None:
        df, table, sel = self._prepare_main_test(data, strategy, pl_dtype, col_type)
        insert_dataframe(df, table, engine)
        result = select_to_dataframe(sel, engine)
        assert_frame_equal(result, df)

    @given(
        engine=sqlite_engines(),
        values=lists(booleans() | none(), min_size=1, max_size=100),
    )
    @mark.parametrize("sr_name", [param("Value"), param("value")])
    def test_sync_snake(
        self, *, engine: Engine, values: list[bool | None], sr_name: str
    ) -> None:
        df, table, sel, expected = self._prepare_snake_test(values, sr_name)
        insert_dataframe(df, table, engine, snake=True)
        result = select_to_dataframe(sel, engine, snake=True)
        assert_frame_equal(result, expected)

    @given(
        engine=sqlite_engines(),
        values=lists(integers(0, 100), max_size=100),
        batch_size=integers(1, 10),
    )
    def test_sync_batch(
        self, *, engine: Engine, values: list[int], batch_size: int
    ) -> None:
        df, table, sel = self._prepare_feature_test(values)
        insert_dataframe(df, table, engine)
        seen: set[int] = set()
        dfs = select_to_dataframe(sel, engine, batch_size=batch_size)
        for df_i in dfs:
            check_polars_dataframe(
                df_i, max_height=batch_size, schema_list={"value": Int64}
            )
            assert df_i["value"].is_in(values).all()
            seen.update(df_i["value"].to_list())
        assert seen == set(values)

    @given(
        data=data(),
        engine=sqlite_engines(),
        values=lists(integers(0, 100), min_size=1, max_size=100, unique=True),
        in_clauses_chunk_size=integers(1, 10),
    )
    def test_sync_in_clauses_non_empty(
        self,
        *,
        data: DataObject,
        engine: Engine,
        values: list[int],
        in_clauses_chunk_size: int,
    ) -> None:
        df, table, sel = self._prepare_feature_test(values)
        insert_dataframe(df, table, engine)
        in_values = data.draw(sets(sampled_from(values)))
        df = select_to_dataframe(
            sel,
            engine,
            in_clauses=(table.c["value"], in_values),
            in_clauses_chunk_size=in_clauses_chunk_size,
        )
        check_polars_dataframe(df, height=len(in_values), schema_list={"value": Int64})
        assert set(df["value"].to_list()) == in_values

    @given(engine=sqlite_engines())
    def test_sync_in_clauses_empty(self, *, engine: Engine) -> None:
        table, sel = self._prepare_empty_test()
        ensure_tables_created(engine, table)
        df = select_to_dataframe(sel, engine, in_clauses=(table.c["value"], []))
        check_polars_dataframe(df, height=0, schema_list={"value": Int64})

    @given(
        data=data(),
        engine=sqlite_engines(),
        batch_size=integers(1, 10),
        values=lists(integers(0, 100), min_size=1, max_size=100, unique=True),
        in_clauses_chunk_size=integers(1, 10),
    )
    def test_sync_batch_and_in_clauses(
        self,
        *,
        data: DataObject,
        batch_size: int,
        engine: Engine,
        values: list[int],
        in_clauses_chunk_size: int,
    ) -> None:
        df = DataFrame({"value": values}, schema={"value": Int64})
        table = self._make_table(Integer)
        insert_dataframe(df, table, engine)
        sel = select(table.c["value"])
        max_height = batch_size * in_clauses_chunk_size
        in_values = data.draw(sets(sampled_from(values)))
        dfs = select_to_dataframe(
            sel,
            engine,
            batch_size=batch_size,
            in_clauses=(table.c["value"], in_values),
            in_clauses_chunk_size=in_clauses_chunk_size,
        )
        seen: set[int] = set()
        for df_i in dfs:
            check_polars_dataframe(
                df_i, max_height=max_height, schema_list={"value": Int64}
            )
            assert df_i["value"].is_in(in_values).all()
            seen.update(df_i["value"].to_list())
        assert seen == in_values

    @given(data=data())
    @mark.parametrize(("strategy", "pl_dtype", "col_type"), cases)
    async def test_async(
        self,
        *,
        data: DataObject,
        strategy: SearchStrategy[Any],
        pl_dtype: PolarsDataType,
        col_type: Any,
    ) -> None:
        df, table, sel = self._prepare_main_test(data, strategy, pl_dtype, col_type)
        engine = await aiosqlite_engines(data)
        await insert_dataframe_async(df, table, engine)
        result = await select_to_dataframe_async(sel, engine)
        assert_frame_equal(result, df)

    @given(data=data(), values=lists(booleans() | none(), min_size=1, max_size=100))
    @mark.parametrize("sr_name", [param("Value"), param("value")])
    async def test_async_snake(
        self, *, data: DataObject, values: list[bool], sr_name: str
    ) -> None:
        df, table, sel, expected = self._prepare_snake_test(values, sr_name)
        engine = await aiosqlite_engines(data)
        await insert_dataframe_async(df, table, engine, snake=True)
        result = await select_to_dataframe_async(sel, engine, snake=True)
        assert_frame_equal(result, expected)

    @given(
        data=data(),
        values=lists(integers(0, 100), min_size=1, max_size=100, unique=True),
        batch_size=integers(1, 10),
    )
    async def test_async_batch(
        self, *, data: DataObject, values: list[int], batch_size: int
    ) -> None:
        df, table, sel = self._prepare_feature_test(values)
        engine = await aiosqlite_engines(data)
        await insert_dataframe_async(df, table, engine)
        seen: set[int] = set()
        dfs = await select_to_dataframe_async(sel, engine, batch_size=batch_size)
        for df_i in dfs:
            check_polars_dataframe(
                df_i, max_height=batch_size, schema_list={"value": Int64}
            )
            assert df_i["value"].is_in(values).all()
            seen.update(df_i["value"].to_list())
        assert seen == set(values)

    @given(
        data=data(),
        values=lists(integers(0, 100), min_size=1, max_size=100, unique=True),
        in_clauses_chunk_size=integers(1, 10),
    )
    async def test_async_in_clauses_non_empty(
        self, *, data: DataObject, values: list[int], in_clauses_chunk_size: int
    ) -> None:
        df, table, sel = self._prepare_feature_test(values)
        engine = await aiosqlite_engines(data)
        await insert_dataframe_async(df, table, engine)
        in_values = data.draw(sets(sampled_from(values)))
        df = await select_to_dataframe_async(
            sel,
            engine,
            in_clauses=(table.c["value"], in_values),
            in_clauses_chunk_size=in_clauses_chunk_size,
        )
        check_polars_dataframe(df, height=len(in_values), schema_list={"value": Int64})
        assert set(df["value"].to_list()) == in_values

    @given(data=data())
    async def test_async_in_clauses_empty(self, *, data: DataObject) -> None:
        table, sel = self._prepare_empty_test()
        engine = await aiosqlite_engines(data)
        await ensure_tables_created_async(engine, table)
        df = await select_to_dataframe_async(
            sel, engine, in_clauses=(table.c["value"], [])
        )
        check_polars_dataframe(df, height=0, schema_list={"value": Int64})

    @given(
        data=data(),
        values=lists(integers(0, 100), min_size=1, max_size=100, unique=True),
        batch_size=integers(1, 10),
        in_clauses_chunk_size=integers(1, 10),
    )
    async def test_async_batch_and_in_clauses(
        self,
        *,
        data: DataObject,
        values: list[int],
        batch_size: int,
        in_clauses_chunk_size: int,
    ) -> None:
        df, table, sel = self._prepare_feature_test(values)
        engine = await aiosqlite_engines(data)
        await insert_dataframe_async(df, table, engine)
        in_values = data.draw(sets(sampled_from(values)))
        max_height = batch_size * in_clauses_chunk_size
        dfs = await select_to_dataframe_async(
            sel,
            engine,
            batch_size=batch_size,
            in_clauses=(table.c["value"], in_values),
            in_clauses_chunk_size=in_clauses_chunk_size,
        )
        seen: set[int] = set()
        async for df_i in dfs:
            check_polars_dataframe(
                df_i, max_height=max_height, schema_list={"value": Int64}
            )
            assert df_i["value"].is_in(values).all()
            seen.update(df_i["value"].to_list())
        assert seen == (in_values)

    def _prepare_main_test(
        self,
        data: DataObject,
        strategy: SearchStrategy[_T],
        pl_dtype: PolarsDataType,
        col_type: Any,
        /,
    ) -> tuple[DataFrame, Table, Select[Any]]:
        values = data.draw(lists(strategy, max_size=100))
        df = DataFrame({"value": values}, schema={"value": pl_dtype})
        table = self._make_table(col_type)
        sel = select(table.c["value"])
        return df, table, sel

    def _make_table(self, type_: Any, /, *, title: bool = False) -> Table:
        return Table(
            "example",
            MetaData(),
            Column("Id" if title else "id", Integer, primary_key=True),
            Column("Value" if title else "value", type_),
        )

    def _prepare_snake_test(
        self, values: Sequence[bool | None], sr_name: str, /
    ) -> tuple[DataFrame, Table, Select[Any], DataFrame]:
        df = DataFrame({sr_name: values}, schema={sr_name: pl.Boolean})
        table = self._make_table(sqlalchemy.Boolean, title=True)
        sel = select(table.c["Value"])
        expected = DataFrame({"value": values}, schema={"value": pl.Boolean})
        return df, table, sel, expected

    def _prepare_feature_test(
        self, values: Sequence[int], /
    ) -> tuple[DataFrame, Table, Select[Any]]:
        df = DataFrame({"value": values}, schema={"value": Int64})
        table = self._make_table(Integer)
        sel = select(table.c["value"])
        return df, table, sel

    def _prepare_empty_test(self, /) -> tuple[Table, Select[Any]]:
        table = self._make_table(Integer)
        sel = select(table.c["value"])
        return table, sel


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
        for col, exp in zip(res.selected_columns, expected, strict=True):
            assert col.name == exp


class TestSelectToDataFrameCheckDuplicates:
    def test_error(self) -> None:
        table = Table("example", MetaData(), Column("id", Integer, primary_key=True))
        sel = select(table.c["id"], table.c["id"])
        with raises(DuplicateColumnError):
            _select_to_dataframe_check_duplicates(sel.selected_columns)


class TestSelectToDataFrameMapSelectToDFSchema:
    def test_main(self) -> None:
        table = Table("example", MetaData(), Column("id", Integer, primary_key=True))
        sel = select(table.c["id"])
        schema = _select_to_dataframe_map_select_to_df_schema(sel)
        expected = {"id": Int64}
        assert schema == expected


class TestSelectToDataFrameMapTableColumnTypeToDType:
    @mark.parametrize(
        ("col_type", "expected"),
        [
            param(BigInteger, Int64),
            param(BIGINT, Int64),
            param(BINARY, Binary),
            param(sqlalchemy.Boolean, pl.Boolean),
            param(BOOLEAN, pl.Boolean),
            param(CHAR, Utf8),
            param(CLOB, Utf8),
            param(sqlalchemy.Date, pl.Date),
            param(DATE, pl.Date),
            param(DECIMAL, Decimal),
            param(Double, Float64),
            param(DOUBLE, Float64),
            param(DOUBLE_PRECISION, Float64),
            param(Float, Float64),
            param(FLOAT, Float64),
            param(INT, Int64),
            param(Integer, Int64),
            param(INTEGER, Int64),
            param(Interval, Duration),
            param(LargeBinary, Binary),
            param(NCHAR, Utf8),
            param(Numeric, Decimal),
            param(NUMERIC, Decimal),
            param(NVARCHAR, Utf8),
            param(REAL, Float64),
            param(SMALLINT, Int64),
            param(SmallInteger, Int64),
            param(String, Utf8),
            param(TEXT, Utf8),
            param(Text, Utf8),
            param(TIME, pl.Time),
            param(sqlalchemy.Time, pl.Time),
            param(Unicode, Utf8),
            param(UnicodeText, Utf8),
            param(Uuid, pl.Utf8),
            param(UUID, pl.Utf8),
            param(VARBINARY, Binary),
            param(VARCHAR, Utf8),
        ],
    )
    @mark.parametrize("use_inst", [param(True), param(False)])
    def test_main(
        self, *, col_type: Any, use_inst: bool, expected: DataTypeClass
    ) -> None:
        col_type_use = col_type() if use_inst else col_type
        dtype = _select_to_dataframe_map_table_column_type_to_dtype(col_type_use)
        assert isinstance(dtype, type)
        assert issubclass(dtype, expected)

    @mark.parametrize("col_type", [param(DATETIME), param(DateTime), param(TIMESTAMP)])
    @mark.parametrize("timezone", [param(None), param(True), param(False)])
    def test_datetime(self, *, col_type: Any, timezone: bool | None) -> None:
        col_type_use = col_type if timezone is None else col_type(timezone=timezone)
        dtype = _select_to_dataframe_map_table_column_type_to_dtype(col_type_use)
        assert isinstance(dtype, Datetime)


class TestSelectToDataFrameYieldSelectsWithInClauses:
    @given(
        engine=sqlite_engines(),
        values=sets(integers(), max_size=100),
        in_clauses_chunk_size=integers(1, 10) | none(),
        chunk_size_frac=floats(0.1, 10.0),
    )
    def test_main(
        self,
        *,
        engine: Engine,
        values: set[int],
        in_clauses_chunk_size: int | None,
        chunk_size_frac: float,
    ) -> None:
        table = Table("example", MetaData(), Column("id", Integer, primary_key=True))
        sel = select(table.c["id"])
        with engine.begin() as conn:
            iterator = _select_to_dataframe_yield_selects_with_in_clauses(
                sel,
                conn,
                (table.c["id"], values),
                in_clauses_chunk_size=in_clauses_chunk_size,
                chunk_size_frac=chunk_size_frac,
            )
            sels = list(iterator)
        for sel in sels:
            assert isinstance(sel, Select)
