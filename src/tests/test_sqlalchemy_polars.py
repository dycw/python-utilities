from __future__ import annotations

import datetime as dt
from operator import eq
from typing import TYPE_CHECKING, Any

import polars as pl
import sqlalchemy
from hypothesis import Phase, given, settings
from hypothesis.strategies import (
    DataObject,
    DrawFn,
    SearchStrategy,
    booleans,
    composite,
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
    uuids,
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
    Utf8,
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
from sqlalchemy.exc import DuplicateColumnError, OperationalError, ProgrammingError

from tests.conftest import FLAKY
from tests.test_sqlalchemy import _upsert_lists
from utilities.datetime import is_equal_mod_tz
from utilities.hypothesis import (
    int32s,
    sets_fixed_length,
    sqlalchemy_engines,
    text_ascii,
)
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
from utilities.zoneinfo import UTC

if TYPE_CHECKING:
    import uuid
    from collections.abc import Callable, Iterable, Sequence

    from polars._typing import PolarsDataType
    from polars.datatypes import DataTypeClass
    from sqlalchemy.ext.asyncio import AsyncEngine

    from utilities.hashlib import md5_hash


_CASES_SELECT: list[
    tuple[SearchStrategy[Any], PolarsDataType, Any, Callable[[Any, Any], bool]]
] = [
    (booleans() | none(), pl.Boolean, sqlalchemy.Boolean, eq),
    (dates() | none(), pl.Date, sqlalchemy.Date, eq),
    (datetimes() | none(), Datetime, DateTime, eq),
    (
        datetimes(timezones=just(UTC)) | none(),
        DatetimeUTC,
        DateTime(timezone=True),
        is_equal_mod_tz,
    ),
    (floats(allow_nan=False) | none(), Float64, Float, is_equal),
    (integers(-10, 10) | none(), Int64, Integer, eq),
    (text_ascii() | none(), Utf8, String, eq),
]
_CASES_INSERT: list[
    tuple[SearchStrategy[Any], PolarsDataType, Any, Callable[[Any, Any], bool]]
] = [*_CASES_SELECT, (integers(-10, 10) | none(), Int32, Integer, eq)]


@composite
def _upsert_dataframes(
    draw: DrawFn,
    /,
    *,
    nullable: bool = False,
    min_height: int = 0,
    max_height: int | None = None,
) -> DataFrame:
    values = draw(
        _upsert_lists(nullable=nullable, min_size=min_height, max_size=max_height)
    )
    return DataFrame(
        values,
        schema={"id_": Int64, "init": pl.Boolean, "post": pl.Boolean},
        orient="row",
    )


class TestInsertDataFrame:
    @FLAKY
    @given(data=data(), name=uuids(), case=sampled_from(_CASES_INSERT))
    @settings(phases={Phase.generate})
    async def test_main(
        self,
        *,
        data: DataObject,
        name: uuid.UUID,
        case: tuple[
            SearchStrategy[Any], PolarsDataType, Any, Callable[[Any, Any], bool]
        ],
    ) -> None:
        strategy, pl_dtype, col_type, check = case
        values = data.draw(lists(strategy, max_size=100))
        df = DataFrame({"value": values}, schema={"value": pl_dtype})
        table = self._make_table(name, col_type)
        engine = await sqlalchemy_engines(data, table)
        await insert_dataframe(df, table, engine)
        sel = select(table.c["value"])
        async with engine.begin() as conn:
            results = (await conn.execute(sel)).scalars().all()
        for r, v in zip(results, values, strict=True):
            assert ((r is None) == (v is None)) or check(r, v)

    @FLAKY
    @given(data=data(), name=uuids(), value=booleans())
    @settings(phases={Phase.generate})
    async def test_assume_exists(
        self, *, data: DataObject, name: uuid.UUID, value: bool
    ) -> None:
        df = DataFrame([(value,)], schema={"value": pl.Boolean})
        table = self._make_table(name, sqlalchemy.Boolean)
        engine = await sqlalchemy_engines(data, table)
        with raises(
            (OperationalError, ProgrammingError), match="(no such table|does not exist)"
        ):
            await insert_dataframe(df, table, engine, assume_tables_exist=True)

    @FLAKY
    @given(data=data(), name=uuids(), value=booleans())
    @settings(phases={Phase.generate})
    async def test_error(
        self, *, data: DataObject, name: uuid.UUID, value: bool
    ) -> None:
        df = DataFrame([(value,)], schema={"other": pl.Boolean})
        table = self._make_table(name, sqlalchemy.Boolean)
        engine = await sqlalchemy_engines(data, table)
        with raises(
            InsertDataFrameError,
            match="Non-empty DataFrame must resolve to at least 1 item",
        ):
            _ = await insert_dataframe(df, table, engine)

    def _make_table(self, name: uuid.UUID, type_: Any, /) -> Table:
        return Table(
            f"test_{name}",
            MetaData(),
            Column("id", Integer, primary_key=True),
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
            param(Utf8, str, True),
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
            match=r"Unable to map DataFrame column 'b' into table schema \{.*\} with snake=True",
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
    @FLAKY
    @given(
        data=data(),
        name=uuids(),
        case=sampled_from([
            (strategy, pl_dtype, col_type)
            for (strategy, pl_dtype, col_type, _) in _CASES_SELECT
        ]),
    )
    @settings(phases={Phase.generate})
    async def test_main(
        self,
        *,
        data: DataObject,
        name: uuid.UUID,
        case: tuple[SearchStrategy[Any], PolarsDataType, Any],
    ) -> None:
        strategy, pl_dtype, col_type = case
        values = data.draw(lists(strategy, max_size=100))
        df = DataFrame({"value": values}, schema={"value": pl_dtype})
        table = self._make_table(name, col_type)
        engine = await sqlalchemy_engines(data, table)
        await insert_dataframe(df, table, engine)
        sel = select(table.c["value"])
        result = await select_to_dataframe(sel, engine)
        assert_frame_equal(result, df)

    @FLAKY
    @given(
        data=data(),
        name=uuids(),
        values=lists(booleans() | none(), min_size=1, max_size=100),
        sr_name=sampled_from(["Value", "value"]),
    )
    @settings(phases={Phase.generate})
    async def test_snake(
        self, *, data: DataObject, name: uuid.UUID, values: list[bool], sr_name: str
    ) -> None:
        df = DataFrame({sr_name: values}, schema={sr_name: pl.Boolean})
        table = self._make_table(name, sqlalchemy.Boolean, title=True)
        engine = await sqlalchemy_engines(data, table)
        await insert_dataframe(df, table, engine, snake=True)
        sel = select(table.c["Value"])
        result = await select_to_dataframe(sel, engine, snake=True)
        expected = DataFrame({"value": values}, schema={"value": pl.Boolean})
        assert_frame_equal(result, expected)

    @FLAKY
    @given(
        data=data(),
        name=uuids(),
        values=lists(integers(0, 100), min_size=1, max_size=100, unique=True),
        batch_size=integers(1, 10),
    )
    @settings(phases={Phase.generate})
    async def test_batch(
        self, *, data: DataObject, name: uuid.UUID, values: list[int], batch_size: int
    ) -> None:
        df, table, engine, sel = await self._prepare_feature_test(data, name, values)
        await insert_dataframe(df, table, engine)
        dfs = await select_to_dataframe(sel, engine, batch_size=batch_size)
        self._assert_batch_results(dfs, batch_size, values)

    @FLAKY
    @given(
        data=data(),
        name=uuids(),
        values=lists(integers(0, 100), min_size=1, max_size=100, unique=True),
        in_clauses_chunk_size=integers(1, 10),
    )
    @settings(phases={Phase.generate})
    async def test_in_clauses_non_empty(
        self,
        *,
        data: DataObject,
        name: uuid.UUID,
        values: list[int],
        in_clauses_chunk_size: int,
    ) -> None:
        df, table, engine, sel = await self._prepare_feature_test(data, name, values)
        await insert_dataframe(df, table, engine)
        in_values = data.draw(sets(sampled_from(values)))
        df = await select_to_dataframe(
            sel,
            engine,
            in_clauses=(table.c["value"], in_values),
            in_clauses_chunk_size=in_clauses_chunk_size,
        )
        check_polars_dataframe(df, height=len(in_values), schema_list={"value": Int64})
        assert set(df["value"].to_list()) == in_values

    @FLAKY
    @given(data=data(), name=uuids())
    @settings(phases={Phase.generate})
    async def test_in_clauses_empty(self, *, data: DataObject, name: uuid.UUID) -> None:
        table = self._make_table(name, Integer)
        engine = await sqlalchemy_engines(data, table)
        await ensure_tables_created(engine, table)
        sel = select(table.c["value"])
        df = await select_to_dataframe(sel, engine, in_clauses=(table.c["value"], []))
        check_polars_dataframe(df, height=0, schema_list={"value": Int64})

    @FLAKY
    @given(
        data=data(),
        name=uuids(),
        values=lists(integers(0, 100), min_size=1, max_size=100, unique=True),
        batch_size=integers(1, 10),
        in_clauses_chunk_size=integers(1, 10),
    )
    @settings(phases={Phase.generate})
    async def test_batch_and_in_clauses(
        self,
        *,
        data: DataObject,
        name: uuid.UUID,
        values: list[int],
        batch_size: int,
        in_clauses_chunk_size: int,
    ) -> None:
        df, table, engine, sel = await self._prepare_feature_test(data, name, values)
        await insert_dataframe(df, table, engine)
        in_values = data.draw(sets(sampled_from(values)))
        async_dfs = await select_to_dataframe(
            sel,
            engine,
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

    def _make_table(
        self, name: uuid.UUID, type_: Any, /, *, title: bool = False
    ) -> Table:
        return Table(
            f"test_{name}",
            MetaData(),
            Column("Id" if title else "id", Integer, primary_key=True),
            Column("Value" if title else "value", type_),
        )

    async def _prepare_feature_test(
        self, data: DataObject, name: uuid.UUID, values: Sequence[int], /
    ) -> tuple[DataFrame, Table, AsyncEngine, Select[Any]]:
        df = DataFrame({"value": values}, schema={"value": Int64})
        table = self._make_table(name, Integer)
        engine = await sqlalchemy_engines(data, table)
        sel = select(table.c["value"])
        return df, table, engine, sel


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
            param(sqlalchemy.UUID, pl.Utf8),
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
        data=data(),
        name=uuids(),
        values=sets(integers(), max_size=100),
        in_clauses_chunk_size=integers(1, 10) | none(),
        chunk_size_frac=floats(0.1, 10.0),
    )
    async def test_main(
        self,
        *,
        data: DataObject,
        name: uuid.UUID,
        values: set[int],
        in_clauses_chunk_size: int | None,
        chunk_size_frac: float,
    ) -> None:
        table = Table(
            f"test_{name}", MetaData(), Column("id", Integer, primary_key=True)
        )
        engine = await sqlalchemy_engines(data, table)
        sel = select(table.c["id"])
        async with engine.begin() as conn:
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


if 0:

    class TestUpsertDataFrame:
        @given(df=_upsert_dataframes(), engine=sqlite_engines())
        def test_sync(self, *, df: DataFrame, engine: Engine) -> None:
            key = TestUpsertDataFrame.test_sync.__qualname__
            name = f"test_{md5_hash(key)}"
            table = self._get_table(name)
            upsert_dataframe(
                df.select("id_", col("init").alias("value")), table, engine
            )
            with engine.begin() as conn:
                res = conn.execute(select(table)).all()
            self._assert_results(res, df.select("id_", "init"))
            upsert_dataframe(
                df.select("id_", col("post").alias("value")).drop_nulls(), table, engine
            )
            with engine.begin() as conn:
                res = conn.execute(select(table)).all()
            expected = df.select(
                "id_", when(col("post").is_null()).then("init").otherwise("post")
            )
            self._assert_results(res, expected)

        @given(engine=sqlite_engines())
        def test_sync_assume_exists(self, *, engine: Engine) -> None:
            key = TestUpsertDataFrame.test_sync_assume_exists.__qualname__
            name = f"test_{md5_hash(key)}"
            df = DataFrame(schema={"value": pl.Boolean})
            table = self._get_table(name)
            ensure_tables_created(engine, table)
            upsert_dataframe(df, table, engine, assume_tables_exist=True)

        @given(
            engine=sqlite_engines(),
            ids=sets_fixed_length(int32s(), 2).map(tuple),
            value1=booleans() | none(),
            value2=booleans() | none(),
        )
        def test_sync_both_nulls_and_non_nulls(
            self,
            *,
            engine: Engine,
            ids: tuple[int, int],
            value1: bool | None,
            value2: bool | None,
        ) -> None:
            key = TestUpsertDataFrame.test_sync_both_nulls_and_non_nulls.__qualname__
            name = f"test_{md5_hash(key)}"
            table = self._get_table(name)
            id1, id2 = ids
            df = DataFrame(
                data=[(id1, value1), (id2, value2)],
                schema={"id_": Int64, "value": pl.Boolean},
                orient="row",
            )
            upsert_dataframe(df, table, engine)

        @given(df=_upsert_dataframes(min_height=1), engine=sqlite_engines())
        def test_sync_error(self, *, df: DataFrame, engine: Engine) -> None:
            key = TestUpsertDataFrame.test_sync_error.__qualname__
            name = f"test_{md5_hash(key)}"
            table = self._get_table(name)
            with raises(
                UpsertDataFrameError,
                match="Non-empty DataFrame must resolve to at least 1 item",
            ):
                upsert_dataframe(
                    df.select(
                        col("id_").alias("not_id"), col("init").alias("not_value")
                    ),
                    table,
                    engine,
                )

        @given(data=data(), df=_upsert_dataframes())
        async def test_async(self, *, data: DataObject, df: DataFrame) -> None:
            key = TestUpsertDataFrame.test_async.__qualname__
            name = f"test_{md5_hash(key)}"
            table = self._get_table(name)
            engine = await aiosqlite_engines(data)
            await upsert_dataframe(
                df.select("id_", col("init").alias("value")), table, engine
            )
            async with engine.begin() as conn:
                res = (await conn.execute(select(table))).all()
            self._assert_results(res, df.select("id_", "init"))
            await upsert_dataframe(
                df.select("id_", col("post").alias("value")).drop_nulls(), table, engine
            )
            async with engine.begin() as conn:
                res = (await conn.execute(select(table))).all()
            expected = df.select(
                "id_", when(col("post").is_null()).then("init").otherwise("post")
            )
            self._assert_results(res, expected)

        @given(data=data())
        async def test_async_assume_exists(self, *, data: DataObject) -> None:
            key = TestUpsertDataFrame.test_async_assume_exists.__qualname__
            name = f"test_{md5_hash(key)}"
            df = DataFrame(schema={"value": pl.Boolean})
            table = self._get_table(name)
            engine = await aiosqlite_engines(data)
            await ensure_tables_created(engine, table)
            await upsert_dataframe(df, table, engine, assume_tables_exist=True)

        @given(
            data=data(),
            ids=sets_fixed_length(int32s(), 2).map(tuple),
            value1=booleans() | none(),
            value2=booleans() | none(),
        )
        async def test_async_both_nulls_and_non_nulls(
            self,
            *,
            data: DataObject,
            ids: tuple[int, int],
            value1: bool | None,
            value2: bool | None,
        ) -> None:
            key = TestUpsertDataFrame.test_async_both_nulls_and_non_nulls.__qualname__
            name = f"test_{md5_hash(key)}"
            table = self._get_table(name)
            engine = await aiosqlite_engines(data)
            id1, id2 = ids
            df = DataFrame(
                data=[(id1, value1), (id2, value2)],
                schema={"id_": Int64, "value": pl.Boolean},
                orient="row",
            )
            await upsert_dataframe(df, table, engine)

        @given(data=data(), df=_upsert_dataframes(min_height=1))
        async def test_async_error(self, *, data: DataObject, df: DataFrame) -> None:
            key = TestUpsertDataFrame.test_async_error.__qualname__
            name = f"test_{md5_hash(key)}"
            table = self._get_table(name)
            engine = await aiosqlite_engines(data)
            with raises(
                UpsertDataFrameAsyncError,
                match="Non-empty DataFrame must resolve to at least 1 item",
            ):
                await upsert_dataframe(
                    df.select(
                        col("id_").alias("not_id"), col("init").alias("not_value")
                    ),
                    table,
                    engine,
                )

        def _get_table(self, name: str, /) -> Table:
            return Table(
                name,
                MetaData(),
                Column("id_", Integer, primary_key=True),
                Column("value", sqlalchemy.Boolean),
            )

        async def _run_test_async(
            self,
            df: DataFrame,
            table: Table,
            engine: AsyncEngine,
            /,
            *,
            snake: bool = False,
            sr_name: str = "value",
        ) -> None:
            await upsert_dataframe(
                df.select("id_", col("init").alias(sr_name)), table, engine, snake=snake
            )
            async with engine.begin() as conn:
                res = (await conn.execute(select(table))).all()
            self._assert_results(res, df.select("id_", "init"))
            await upsert_dataframe(
                df.select("id_", col("post").alias(sr_name)).drop_nulls(),
                table,
                engine,
                snake=snake,
            )
            async with engine.begin() as conn:
                res = (await conn.execute(select(table))).all()
            expected = df.select(
                "id_", when(col("post").is_null()).then("init").otherwise("post")
            )
            self._assert_results(res, expected)

        def _assert_results(
            self, results: Sequence[Any], expected: DataFrame, /
        ) -> None:
            assert set(results) == set(expected.rows())
