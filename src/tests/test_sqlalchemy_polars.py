from __future__ import annotations

import datetime as dt
import re
from operator import eq
from re import DOTALL
from typing import TYPE_CHECKING, Any

import polars as pl
import sqlalchemy
from hypothesis import HealthCheck, Phase, given, settings
from hypothesis.strategies import (
    DataObject,
    SearchStrategy,
    booleans,
    data,
    dates,
    datetimes,
    floats,
    integers,
    lists,
    none,
    sampled_from,
    sets,
)
from polars import (
    Binary,
    DataFrame,
    Datetime,
    Decimal,
    Duration,
    Float64,
    Int32,
    Int64,
    UInt32,
    UInt64,
    col,
    when,
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
    Float,
    Integer,
    Interval,
    LargeBinary,
    MetaData,
    Numeric,
    Select,
    SmallInteger,
    Table,
    Text,
    Unicode,
    UnicodeText,
    Uuid,
    select,
)
from sqlalchemy.exc import DuplicateColumnError, OperationalError, ProgrammingError

from tests.test_sqlalchemy import _table_names, _upsert_lists
from utilities.hypothesis import int32s, text_ascii, zoned_datetimes
from utilities.math import is_equal
from utilities.polars import DatetimeUTC, check_polars_dataframe
from utilities.sqlalchemy import ensure_tables_created
from utilities.sqlalchemy_polars import (
    InsertDataFrameError,
    _insert_dataframe_check_df_and_db_types,
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
    select_to_dataframe,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Sequence

    from polars._typing import PolarsDataType
    from polars.datatypes import DataTypeClass
    from sqlalchemy.ext.asyncio import AsyncEngine


_CASES_SELECT: list[
    tuple[SearchStrategy[Any], PolarsDataType, Any, Callable[[Any, Any], bool]]
] = [
    (booleans() | none(), pl.Boolean, sqlalchemy.Boolean, eq),
    (dates() | none(), pl.Date, sqlalchemy.Date, eq),
    (datetimes() | none(), Datetime, DateTime, eq),
    (
        zoned_datetimes().map(lambda d: d.py_datetime()) | none(),
        DatetimeUTC,
        DateTime(timezone=True),
        eq,
    ),
    (floats(allow_nan=False) | none(), Float64, Float, is_equal),
    (int32s() | none(), Int64, Integer, eq),
    (text_ascii() | none(), pl.String, sqlalchemy.String, eq),
]
_CASES_INSERT: list[
    tuple[SearchStrategy[Any], PolarsDataType, Any, Callable[[Any, Any], bool]]
] = [*_CASES_SELECT, (int32s() | none(), Int32, Integer, eq)]


class TestInsertDataFrame:
    @given(data=data())
    @mark.parametrize(("strategy", "pl_dtype", "col_type", "check"), _CASES_INSERT)
    @settings(
        max_examples=1,
        phases={Phase.generate},
        suppress_health_check={HealthCheck.function_scoped_fixture},
    )
    async def test_main(
        self,
        *,
        data: DataObject,
        strategy: SearchStrategy[Any],
        pl_dtype: PolarsDataType,
        col_type: Any,
        check: Callable[[Any, Any], bool],
        test_engine: AsyncEngine,
    ) -> None:
        values = data.draw(lists(strategy, min_size=1))
        df = DataFrame({"value": values}, schema={"value": pl_dtype})
        table = self._make_table(col_type)
        await insert_dataframe(df, table, test_engine)
        sel = select(table.c["value"])
        async with test_engine.begin() as conn:
            results = (await conn.execute(sel)).scalars().all()
        for r, v in zip(results, values, strict=True):
            assert ((r is None) == (v is None)) or check(r, v)

    async def test_assume_exists_with_empty(self, *, test_engine: AsyncEngine) -> None:
        df = DataFrame(schema={"value": pl.Boolean})
        table = self._make_table(sqlalchemy.Boolean)
        await ensure_tables_created(test_engine, table)
        await insert_dataframe(df, table, test_engine, assume_tables_exist=True)

    @mark.parametrize("value", [param(True), param(False)])
    async def test_assume_exists_with_non_empty(
        self, *, value: bool, test_engine: AsyncEngine
    ) -> None:
        df = DataFrame([(value,)], schema={"value": pl.Boolean})
        table = self._make_table(sqlalchemy.Boolean)

        with raises(
            (OperationalError, ProgrammingError), match="(no such table|does not exist)"
        ):
            await insert_dataframe(df, table, test_engine, assume_tables_exist=True)

    async def test_empty(self, *, test_engine: AsyncEngine) -> None:
        df = DataFrame(schema={"value": pl.Boolean})
        table = self._make_table(sqlalchemy.Boolean)
        await insert_dataframe(df, table, test_engine, assume_tables_exist=False)
        sel = select(table.c["value"])
        async with test_engine.begin() as conn:
            results = (await conn.execute(sel)).scalars().all()
        assert results == []

    @given(values=_upsert_lists(min_size=1))
    @settings(
        max_examples=1,
        phases={Phase.generate},
        suppress_health_check={HealthCheck.function_scoped_fixture},
    )
    async def test_upsert(
        self, *, values: list[tuple[int, bool, bool | None]], test_engine: AsyncEngine
    ) -> None:
        df = DataFrame(
            values,
            schema={"id_": Int64, "init": pl.Boolean, "post": pl.Boolean},
            orient="row",
        )
        table = self._make_table(sqlalchemy.Boolean)
        await insert_dataframe(
            df.select("id_", col("init").alias("value")),
            table,
            test_engine,
            upsert="selected",
        )
        sel = select(table)
        async with test_engine.begin() as conn:
            results = (await conn.execute(sel)).all()
        assert set(results) == set(df.select("id_", "init").rows())
        await insert_dataframe(
            df.select("id_", col("post").alias("value")).drop_nulls(),
            table,
            test_engine,
            upsert="selected",
        )
        async with test_engine.begin() as conn:
            results = (await conn.execute(sel)).all()
        expected = df.select(
            "id_", when(col("post").is_null()).then("init").otherwise("post")
        )
        assert set(results) == set(expected.rows())

    @mark.parametrize("value", [param(True), param(False)])
    async def test_error(self, *, value: bool, test_engine: AsyncEngine) -> None:
        df = DataFrame([(value,)], schema={"other": pl.Boolean})
        table = self._make_table(sqlalchemy.Boolean)
        with raises(
            InsertDataFrameError,
            match="Non-empty DataFrame must resolve to at least 1 item",
        ):
            _ = await insert_dataframe(df, table, test_engine)

    def _make_table(self, type_: Any, /) -> Table:
        return Table(
            _table_names(),
            MetaData(),
            Column("id_", Integer, primary_key=True),
            Column("value", type_),
        )


class TestInsertDataFrameCheckDFAndDBTypes:
    @mark.parametrize(
        ("dtype", "db_col_type", "expected"),
        [
            param(pl.Boolean, bool, True),
            param(pl.Date, dt.date, True),
            param(pl.Date, dt.datetime, False),
            param(Datetime, dt.date, False),
            param(Datetime, dt.datetime, True),
            param(Float64, float, True),
            param(Float64, int, False),
            param(Int32, int, True),
            param(Int32, float, False),
            param(Int64, int, True),
            param(Int64, float, False),
            param(UInt32, int, True),
            param(UInt32, float, False),
            param(UInt64, int, True),
            param(UInt64, float, False),
            param(pl.String, str, True),
        ],
    )
    def test_main(
        self, *, dtype: PolarsDataType, db_col_type: type, expected: bool
    ) -> None:
        result = _insert_dataframe_check_df_and_db_types(dtype, db_col_type)
        assert result is expected


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
        with raises(
            _InsertDataFrameMapDFColumnToTableColumnAndTypeError,
            match=r"Unable to map DataFrame column 'value' into table schema \{.*\} with snake=[True|False]",
        ):
            _ = _insert_dataframe_map_df_column_to_table_column_and_type(
                "value", schema, snake=snake
            )

    def test_error_non_unique(self) -> None:
        schema = {"a": int, "b": float, "B": float, "c": str}
        with raises(
            _InsertDataFrameMapDFColumnToTableColumnAndTypeError,
            match=re.compile(
                r"Unable to map DataFrame column 'b' into table schema \{.*\} with snake=True",
                flags=DOTALL,
            ),
        ):
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
        df_schema = {"a": Int64, "b": Float64, "c": pl.String}
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
            Column("c", sqlalchemy.String),
        )
        result = _insert_dataframe_map_df_schema_to_table(df_schema, table)
        expected = {"a": "a", "b": "b"}
        assert result == expected


class TestSelectToDataFrame:
    @given(data=data())
    @mark.parametrize(
        ("strategy", "pl_dtype", "col_type"),
        [
            (strategy, pl_dtype, col_type)
            for (strategy, pl_dtype, col_type, _) in _CASES_SELECT
        ],
    )
    @settings(
        max_examples=1,
        phases={Phase.generate},
        suppress_health_check={HealthCheck.function_scoped_fixture},
    )
    async def test_main(
        self,
        *,
        data: DataObject,
        strategy: SearchStrategy[Any],
        pl_dtype: PolarsDataType,
        col_type: Any,
        test_engine: AsyncEngine,
    ) -> None:
        values = data.draw(lists(strategy, min_size=1))
        df = DataFrame({"value": values}, schema={"value": pl_dtype})
        table = self._make_table(col_type)
        await insert_dataframe(df, table, test_engine)
        sel = select(table.c["value"])
        result = await select_to_dataframe(sel, test_engine)
        assert_frame_equal(result, df)

    @given(values=lists(booleans() | none(), min_size=1))
    @mark.parametrize("sr_name", [param("Value"), param("value")])
    @settings(
        max_examples=1,
        phases={Phase.generate},
        suppress_health_check={HealthCheck.function_scoped_fixture},
    )
    async def test_snake(
        self, *, values: list[bool], sr_name: str, test_engine: AsyncEngine
    ) -> None:
        df = DataFrame({sr_name: values}, schema={sr_name: pl.Boolean})
        table = self._make_table(sqlalchemy.Boolean, title=True)
        await insert_dataframe(df, table, test_engine, snake=True)
        sel = select(table.c["Value"])
        result = await select_to_dataframe(sel, test_engine, snake=True)
        expected = DataFrame({"value": values}, schema={"value": pl.Boolean})
        assert_frame_equal(result, expected)

    @given(values=lists(int32s(), min_size=1, unique=True), batch_size=integers(1, 10))
    @settings(
        max_examples=1,
        phases={Phase.generate},
        suppress_health_check={HealthCheck.function_scoped_fixture},
    )
    async def test_batch(
        self, *, values: list[int], batch_size: int, test_engine: AsyncEngine
    ) -> None:
        df, table, sel = await self._prepare_feature_test(values)
        await insert_dataframe(df, table, test_engine)
        dfs = await select_to_dataframe(sel, test_engine, batch_size=batch_size)
        self._assert_batch_results(dfs, batch_size, values)

    @given(
        data=data(),
        values=lists(int32s(), min_size=1, unique=True),
        in_clauses_chunk_size=integers(1, 10),
    )
    @settings(
        max_examples=1,
        phases={Phase.generate},
        suppress_health_check={HealthCheck.function_scoped_fixture},
    )
    async def test_in_clauses_non_empty(
        self,
        *,
        data: DataObject,
        values: list[int],
        in_clauses_chunk_size: int,
        test_engine: AsyncEngine,
    ) -> None:
        df, table, sel = await self._prepare_feature_test(values)
        await insert_dataframe(df, table, test_engine)
        in_values = data.draw(sets(sampled_from(values), min_size=1))
        df = await select_to_dataframe(
            sel,
            test_engine,
            in_clauses=(table.c["value"], in_values),
            in_clauses_chunk_size=in_clauses_chunk_size,
        )
        check_polars_dataframe(df, height=len(in_values), schema_list={"value": Int64})
        assert set(df["value"].to_list()) == in_values

    async def test_in_clauses_empty(self, *, test_engine: AsyncEngine) -> None:
        table = self._make_table(Integer)
        await ensure_tables_created(test_engine, table)
        sel = select(table.c["value"])
        df = await select_to_dataframe(
            sel, test_engine, in_clauses=(table.c["value"], [])
        )
        check_polars_dataframe(df, height=0, schema_list={"value": Int64})

    @given(
        data=data(),
        values=lists(int32s(), min_size=1, unique=True),
        batch_size=integers(1, 10),
        in_clauses_chunk_size=integers(1, 10),
    )
    @settings(
        max_examples=1,
        phases={Phase.generate},
        suppress_health_check={HealthCheck.function_scoped_fixture},
    )
    async def test_batch_and_in_clauses(
        self,
        *,
        data: DataObject,
        values: list[int],
        batch_size: int,
        in_clauses_chunk_size: int,
        test_engine: AsyncEngine,
    ) -> None:
        df, table, sel = await self._prepare_feature_test(values)
        await insert_dataframe(df, table, test_engine)
        in_values = data.draw(sets(sampled_from(values), min_size=1))
        async_dfs = await select_to_dataframe(
            sel,
            test_engine,
            batch_size=batch_size,
            in_clauses=(table.c["value"], in_values),
            in_clauses_chunk_size=in_clauses_chunk_size,
        )
        sync_dfs = [df async for df in async_dfs]
        max_height = batch_size * in_clauses_chunk_size
        self._assert_batch_results(sync_dfs, max_height, in_values)

    def _assert_batch_results(
        self, dfs: Iterable[DataFrame], max_height: int, values: Iterable[int], /
    ) -> None:
        seen: set[int] = set()
        values = set(values)
        for df_i in dfs:
            check_polars_dataframe(
                df_i, max_height=max_height, schema_list={"value": Int64}
            )
            assert df_i["value"].is_in(values).all()
            seen.update(df_i["value"].to_list())
        assert seen == values

    def _make_table(self, type_: Any, /, *, title: bool = False) -> Table:
        return Table(
            _table_names(),
            MetaData(),
            Column("Id" if title else "id", Integer, primary_key=True),
            Column("Value" if title else "value", type_),
        )

    async def _prepare_feature_test(
        self, values: Sequence[int], /
    ) -> tuple[DataFrame, Table, Select[Any]]:
        df = DataFrame({"value": values}, schema={"value": Int64})
        table = self._make_table(Integer)
        sel = select(table.c["value"])
        return df, table, sel


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
        for column, exp in zip(res.selected_columns, expected, strict=True):
            assert column.name == exp


class TestSelectToDataFrameCheckDuplicates:
    def test_error(self) -> None:
        table = Table("example", MetaData(), Column("id", Integer, primary_key=True))
        sel = select(table.c["id"], table.c["id"])
        with raises(
            DuplicateColumnError,
            match=r"Columns must not contain duplicates; got \{'id': 2\}",
        ):
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
            param(CHAR, pl.String),
            param(CLOB, pl.String),
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
            param(NCHAR, pl.String),
            param(Numeric, Decimal),
            param(NUMERIC, Decimal),
            param(NVARCHAR, pl.String),
            param(REAL, Float64),
            param(SMALLINT, Int64),
            param(SmallInteger, Int64),
            param(sqlalchemy.String, pl.String),
            param(TEXT, pl.String),
            param(Text, pl.String),
            param(TIME, pl.Time),
            param(sqlalchemy.Time, pl.Time),
            param(Unicode, pl.String),
            param(UnicodeText, pl.String),
            param(Uuid, pl.String),
            param(UUID, pl.String),
            param(VARBINARY, Binary),
            param(VARCHAR, pl.String),
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
        values=sets(int32s(), min_size=1),
        in_clauses_chunk_size=integers(1, 10) | none(),
        chunk_size_frac=floats(0.1, 10.0),
    )
    @settings(
        max_examples=1,
        phases={Phase.generate},
        suppress_health_check={HealthCheck.function_scoped_fixture},
    )
    async def test_main(
        self,
        *,
        values: set[int],
        in_clauses_chunk_size: int | None,
        chunk_size_frac: float,
        test_engine: AsyncEngine,
    ) -> None:
        table = Table(
            _table_names(), MetaData(), Column("id", Integer, primary_key=True)
        )
        sel = select(table.c["id"])
        sels = _select_to_dataframe_yield_selects_with_in_clauses(
            sel,
            test_engine,
            (table.c["id"], values),
            in_clauses_chunk_size=in_clauses_chunk_size,
            chunk_size_frac=chunk_size_frac,
        )
        for sel in sels:
            assert isinstance(sel, Select)
