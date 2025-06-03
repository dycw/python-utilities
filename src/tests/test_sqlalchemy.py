from __future__ import annotations

from dataclasses import dataclass, field
from itertools import chain
from time import time_ns
from typing import TYPE_CHECKING, Any, Literal, cast, overload, override
from uuid import uuid4

from hypothesis import Phase, assume, given, settings
from hypothesis.strategies import (
    DataObject,
    SearchStrategy,
    booleans,
    data,
    floats,
    integers,
    lists,
    none,
    sampled_from,
    sets,
    tuples,
)
from pytest import mark, param, raises
from sqlalchemy import Boolean, Column, Integer, MetaData, Table, select
from sqlalchemy.exc import DatabaseError, OperationalError, ProgrammingError
from sqlalchemy.ext.asyncio import AsyncEngine
from sqlalchemy.orm import DeclarativeBase, Mapped, MappedAsDataclass, mapped_column

from tests.test_asyncio_classes.loopers import _BACKOFF, _FREQ, assert_looper_stats
from utilities.asyncio import Looper
from utilities.hypothesis import (
    int32s,
    pairs,
    settings_with_reduced_examples,
    sqlalchemy_engines,
    temp_paths,
)
from utilities.iterables import one
from utilities.modules import is_installed
from utilities.sqlalchemy import (
    CheckEngineError,
    Dialect,
    GetTableError,
    InsertItemsError,
    TablenameMixin,
    TableOrORMInstOrClass,
    Upserter,
    UpserterError,
    UpsertItemsError,
    UpsertService,
    UpsertServiceMixin,
    _get_dialect,
    _get_dialect_max_params,
    _InsertItem,
    _is_pair_of_sequence_of_tuple_or_string_mapping_and_table,
    _is_pair_of_str_mapping_and_table,
    _is_pair_of_tuple_and_table,
    _is_pair_of_tuple_or_str_mapping_and_table,
    _map_mapping_to_table,
    _MapMappingToTableExtraColumnsError,
    _MapMappingToTableSnakeMapEmptyError,
    _MapMappingToTableSnakeMapNonUniqueError,
    _normalize_insert_item,
    _normalize_upsert_item,
    _NormalizedItem,
    _NormalizeInsertItemError,
    _orm_inst_to_dict,
    _prepare_insert_or_upsert_items,
    _prepare_insert_or_upsert_items_merge_items,
    _PrepareInsertOrUpsertItemsError,
    _SelectedOrAll,
    _tuple_to_mapping,
    check_engine,
    columnwise_max,
    columnwise_min,
    create_async_engine,
    ensure_tables_created,
    ensure_tables_dropped,
    get_chunk_size,
    get_column_names,
    get_columns,
    get_primary_key_values,
    get_table,
    get_table_name,
    hash_primary_key_values,
    insert_items,
    is_orm,
    is_table_or_orm,
    migrate_data,
    selectable_to_string,
    upsert_items,
    yield_primary_key_columns,
)
from utilities.text import strip_and_dedent
from utilities.typing import get_args

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator
    from pathlib import Path

    from utilities.types import Duration, StrMapping


def _table_names() -> str:
    """Generate at unique string."""
    now = time_ns()
    key = str(uuid4()).replace("-", "")
    return f"{now}_{key}"


@overload
def _upsert_triples(
    *, nullable: Literal[True]
) -> SearchStrategy[tuple[int, bool, bool]]: ...
@overload
def _upsert_triples(
    *, nullable: bool = ...
) -> SearchStrategy[tuple[int, bool, bool | None]]: ...
def _upsert_triples(
    *, nullable: bool = False
) -> SearchStrategy[tuple[int, bool, bool | None]]:
    elements = booleans()
    if nullable:
        elements |= none()
    return tuples(int32s(), booleans(), elements)


def _upsert_lists(
    *, nullable: bool = False, min_size: int = 0, max_size: int | None = None
) -> SearchStrategy[list[tuple[int, bool, bool | None]]]:
    return lists(
        _upsert_triples(nullable=nullable),
        min_size=min_size,
        max_size=max_size,
        unique_by=lambda x: x[0],
    )


class TestCheckEngine:
    @given(data=data())
    @settings_with_reduced_examples(phases={Phase.generate})
    async def test_main(self, *, data: DataObject) -> None:
        engine = await sqlalchemy_engines(data)
        await check_engine(engine)

    @given(data=data())
    @settings_with_reduced_examples(phases={Phase.generate})
    async def test_num_tables_pass(self, *, data: DataObject) -> None:
        table = Table(
            _table_names(), MetaData(), Column("id", Integer, primary_key=True)
        )
        engine = await sqlalchemy_engines(data, table)
        await ensure_tables_created(engine, table)
        match _get_dialect(engine):
            case "sqlite":
                expected = 1
            case "postgresql":
                expected = (int(1e6), 1.0)
            case _ as dialect:
                raise NotImplementedError(dialect)
        await check_engine(engine, num_tables=expected)

    @given(data=data())
    @settings_with_reduced_examples(phases={Phase.generate})
    async def test_num_tables_error(self, *, data: DataObject) -> None:
        engine = await sqlalchemy_engines(data)
        with raises(CheckEngineError, match=r".* must have 100000 table\(s\); got .*"):
            await check_engine(engine, num_tables=100000)


class TestColumnwiseMinMax:
    @given(data=data(), values=sets(pairs(integers(0, 10) | none()), min_size=1))
    @settings_with_reduced_examples(phases={Phase.generate})
    async def test_main(
        self, *, data: DataObject, values: set[tuple[int | None, int | None]]
    ) -> None:
        table = Table(
            _table_names(),
            MetaData(),
            Column("id_", Integer, primary_key=True, autoincrement=True),
            Column("x", Integer),
            Column("y", Integer),
        )
        engine = await sqlalchemy_engines(data, table)
        await insert_items(engine, ([{"x": x, "y": y} for x, y in values], table))
        sel = select(
            table.c["x"],
            table.c["y"],
            columnwise_min(table.c["x"], table.c["y"]).label("min_xy"),
            columnwise_max(table.c["x"], table.c["y"]).label("max_xy"),
        )
        async with engine.begin() as conn:
            res = (await conn.execute(sel)).all()
        assert len(res) == len(values)
        for x, y, min_xy, max_xy in res:
            if (x is None) and (y is None):
                assert min_xy is None
                assert max_xy is None
            elif (x is not None) and (y is None):
                assert min_xy == x
                assert max_xy == x
            elif (x is None) and (y is not None):
                assert min_xy == y
                assert max_xy == y
            else:
                assert min_xy == min(x, y)
                assert max_xy == max(x, y)

    @given(data=data())
    @settings_with_reduced_examples(phases={Phase.generate})
    async def test_label(self, *, data: DataObject) -> None:
        table = Table(
            _table_names(),
            MetaData(),
            Column("id_", Integer, primary_key=True, autoincrement=True),
            Column("x", Integer),
        )
        engine = await sqlalchemy_engines(data, table)
        await ensure_tables_created(engine, table)
        sel = select(columnwise_min(table.c.x, table.c.x))
        async with engine.begin() as conn:
            _ = (await conn.execute(sel)).all()


class TestCreateAsyncEngine:
    @given(temp_path=temp_paths())
    @settings_with_reduced_examples(phases={Phase.generate})
    def test_async(self, *, temp_path: Path) -> None:
        engine = create_async_engine("sqlite+aiosqlite", database=temp_path.name)
        assert isinstance(engine, AsyncEngine)

    @given(temp_path=temp_paths())
    @settings_with_reduced_examples(phases={Phase.generate})
    def test_query(self, *, temp_path: Path) -> None:
        engine = create_async_engine(
            "sqlite+aiosqlite",
            database=temp_path.name,
            query={"arg1": "value1", "arg2": ["value2"]},
        )
        assert isinstance(engine, AsyncEngine)


class TestEnsureTablesCreated:
    @given(data=data())
    @settings_with_reduced_examples(phases={Phase.generate})
    async def test_table(self, *, data: DataObject) -> None:
        table = Table(
            _table_names(), MetaData(), Column("id_", Integer, primary_key=True)
        )
        engine = await sqlalchemy_engines(data, table)
        await self._run_test(engine, table)

    @given(data=data())
    @settings_with_reduced_examples(phases={Phase.generate})
    async def test_mapped_class(self, *, data: DataObject) -> None:
        class Base(DeclarativeBase, MappedAsDataclass): ...

        class Example(Base):
            __tablename__ = _table_names()

            id_: Mapped[int] = mapped_column(Integer, kw_only=True, primary_key=True)

        engine = await sqlalchemy_engines(data, Example)
        await self._run_test(engine, Example)

    async def _run_test(
        self, engine: AsyncEngine, table_or_orm: TableOrORMInstOrClass, /
    ) -> None:
        for _ in range(2):
            await ensure_tables_created(engine, table_or_orm)
        sel = select(get_table(table_or_orm))
        async with engine.begin() as conn:
            _ = (await conn.execute(sel)).all()


class TestEnsureTablesDropped:
    @given(data=data())
    @settings_with_reduced_examples(phases={Phase.generate})
    async def test_table(self, *, data: DataObject) -> None:
        table = Table(
            _table_names(), MetaData(), Column("id_", Integer, primary_key=True)
        )
        engine = await sqlalchemy_engines(data, table)
        await self._run_test(engine, table)

    @given(data=data())
    @settings_with_reduced_examples(phases={Phase.generate})
    async def test_mapped_class(self, *, data: DataObject) -> None:
        class Base(DeclarativeBase, MappedAsDataclass): ...

        class Example(Base):
            __tablename__ = _table_names()

            id_: Mapped[int] = mapped_column(Integer, kw_only=True, primary_key=True)

        engine = await sqlalchemy_engines(data, Example)
        await self._run_test(engine, Example)

    async def _run_test(
        self, engine: AsyncEngine, table_or_orm: TableOrORMInstOrClass, /
    ) -> None:
        for _ in range(2):
            await ensure_tables_dropped(engine, table_or_orm)
        sel = select(get_table(table_or_orm))
        with raises(DatabaseError):
            async with engine.begin() as conn:
                _ = await conn.execute(sel)


class TestGetChunkSize:
    @given(data=data(), chunk_size_frac=floats(0.0, 1.0), scaling=floats(0.1, 10.0))
    @settings_with_reduced_examples(phases={Phase.generate})
    async def test_main(
        self, *, data: DataObject, chunk_size_frac: float, scaling: float
    ) -> None:
        engine = await sqlalchemy_engines(data)
        result = get_chunk_size(
            engine, chunk_size_frac=chunk_size_frac, scaling=scaling
        )
        assert result >= 1


class TestGetColumnNames:
    def test_table(self) -> None:
        table = Table("example", MetaData(), Column("id_", Integer, primary_key=True))
        self._run_test(table)

    def test_mapped_class(self) -> None:
        class Base(DeclarativeBase, MappedAsDataclass): ...

        class Example(Base):
            __tablename__ = "example"

            id_: Mapped[int] = mapped_column(Integer, kw_only=True, primary_key=True)

        self._run_test(Example)

    def _run_test(self, table_or_orm: TableOrORMInstOrClass, /) -> None:
        assert get_column_names(table_or_orm) == ["id_"]


class TestGetColumns:
    def test_table(self) -> None:
        table = Table("example", MetaData(), Column("id", Integer, primary_key=True))
        self._run_test(table)

    def test_mapped_class(self) -> None:
        class Base(DeclarativeBase, MappedAsDataclass): ...

        class Example(Base):
            __tablename__ = "example"

            id_: Mapped[int] = mapped_column(Integer, kw_only=True, primary_key=True)

        self._run_test(Example)

    def _run_test(self, table_or_orm: TableOrORMInstOrClass, /) -> None:
        columns = get_columns(table_or_orm)
        assert isinstance(columns, list)
        assert len(columns) == 1
        assert isinstance(columns[0], Column)


class TestGetDialect:
    @mark.skipif(condition=not is_installed("pyodbc"), reason="'pyodbc' not installed")
    def test_mssql(self) -> None:
        engine = create_async_engine("mssql")
        assert _get_dialect(engine) == "mssql"

    @mark.skipif(
        condition=not is_installed("mysqldb"), reason="'mysqldb' not installed"
    )
    def test_mysql(self) -> None:
        engine = create_async_engine("mysql")
        assert _get_dialect(engine) == "mysql"

    @mark.skipif(
        condition=not is_installed("oracledb"), reason="'oracledb' not installed"
    )
    def test_oracle(self) -> None:
        engine = create_async_engine("oracle+oracledb")
        assert _get_dialect(engine) == "oracle"

    @mark.skipif(
        condition=not is_installed("asyncpg"), reason="'asyncpg' not installed"
    )
    def test_postgres(self) -> None:
        engine = create_async_engine("postgresql+asyncpg")
        assert _get_dialect(engine) == "postgresql"

    @mark.skipif(
        condition=not is_installed("aiosqlite"), reason="'asyncpg' not installed"
    )
    def test_sqlite(self) -> None:
        engine = create_async_engine("sqlite+aiosqlite")
        assert _get_dialect(engine) == "sqlite"


class TestGetDialectMaxParams:
    @given(dialect=sampled_from(get_args(Dialect)))
    def test_max_params(self, *, dialect: Dialect) -> None:
        max_params = _get_dialect_max_params(dialect)
        assert isinstance(max_params, int)


class TestGetPrimaryKeyValues:
    @given(id1=integers(), id2=integers(), value=booleans())
    def test_main(self, *, id1: int, id2: int, value: bool) -> None:
        class Base(DeclarativeBase, MappedAsDataclass): ...

        class Example(Base):
            __tablename__ = "example"

            id1: Mapped[int] = mapped_column(Integer, kw_only=True, primary_key=True)
            id2: Mapped[int] = mapped_column(Integer, kw_only=True, primary_key=True)
            value: Mapped[bool] = mapped_column(Boolean, kw_only=True, nullable=False)

        obj = Example(id1=id1, id2=id2, value=value)
        result = get_primary_key_values(obj)
        expected = (id1, id2)
        assert result == expected


class TestGetTable:
    def test_table(self) -> None:
        table = Table("example", MetaData(), Column("id_", Integer, primary_key=True))
        result = get_table(table)
        assert result is table

    def test_mapped_class(self) -> None:
        class Base(DeclarativeBase, MappedAsDataclass): ...

        class Example(Base):
            __tablename__ = "example"

            id: Mapped[int] = mapped_column(Integer, kw_only=True, primary_key=True)

        result = get_table(Example)
        expected = Example.__table__
        assert result is expected

    def test_instance_of_mapped_class(self) -> None:
        class Base(DeclarativeBase, MappedAsDataclass): ...

        class Example(Base):
            __tablename__ = "example"

            id: Mapped[int] = mapped_column(Integer, kw_only=True, primary_key=True)

        obj = Example(id=1)
        result = get_table(obj)
        expected = Example.__table__
        assert result is expected

    def test_error(self) -> None:
        with raises(
            GetTableError, match="Object .* must be a Table or mapped class; got .*"
        ):
            _ = get_table(cast("Any", type(None)))


class TestGetTableName:
    def test_table(self) -> None:
        table = Table("example", MetaData(), Column("id_", Integer, primary_key=True))
        result = get_table_name(table)
        expected = "example"
        assert result == expected

    def test_mapped_class(self) -> None:
        class Base(DeclarativeBase, MappedAsDataclass): ...

        class Example(Base):
            __tablename__ = "example"

            id_: Mapped[int] = mapped_column(Integer, kw_only=True, primary_key=True)

        result = get_table_name(Example)
        expected = "example"
        assert result == expected


class TestHashPrimaryKeyValues:
    @given(id1=integers(), id2=integers(), value=booleans())
    def test_main(self, *, id1: int, id2: int, value: bool) -> None:
        class Base(DeclarativeBase, MappedAsDataclass): ...

        class Example(Base):
            __tablename__ = "example"

            id1: Mapped[int] = mapped_column(Integer, kw_only=True, primary_key=True)
            id2: Mapped[int] = mapped_column(Integer, kw_only=True, primary_key=True)
            value: Mapped[bool] = mapped_column(Boolean, kw_only=True, nullable=False)

            @override
            def __hash__(self) -> int:
                return hash_primary_key_values(self)

        obj = Example(id1=id1, id2=id2, value=value)
        result = hash(obj)
        expected = hash((id1, id2))
        assert result == expected


class TestInsertItems:
    @given(data=data(), case=sampled_from(["tuple", "dict"]), id_=integers(0, 10))
    @settings_with_reduced_examples(phases={Phase.generate})
    async def test_pair_of_obj_and_table(
        self, *, data: DataObject, case: Literal["tuple", "dict"], id_: int
    ) -> None:
        table = self._make_table()
        engine = await sqlalchemy_engines(data, table)
        match case:
            case "tuple":
                item = (id_,), table
            case "dict":
                item = {"id_": id_}, table
        await self._run_test(engine, table, {id_}, item)

    @given(
        data=data(),
        case=sampled_from([
            "pair-list-of-tuples",
            "pair-list-of-dicts",
            "list-of-pair-of-tuples",
            "list-of-pair-of-dicts",
        ]),
        ids=sets(integers(0, 10), min_size=1),
    )
    @settings_with_reduced_examples(phases={Phase.generate})
    async def test_pair_of_objs_and_table_or_list_of_pairs_of_objs_and_table(
        self,
        *,
        data: DataObject,
        case: Literal[
            "pair-list-of-tuples",
            "pair-list-of-dicts",
            "list-of-pair-of-tuples",
            "list-of-pair-of-dicts",
        ],
        ids: set[int],
    ) -> None:
        table = self._make_table()
        engine = await sqlalchemy_engines(data, table)
        match case:
            case "pair-list-of-tuples":
                item = [((id_,)) for id_ in ids], table
            case "pair-list-of-dicts":
                item = [({"id_": id_}) for id_ in ids], table
            case "list-of-pair-of-tuples":
                item = [((id_,), table) for id_ in ids]
            case "list-of-pair-of-dicts":
                item = [({"id_": id_}, table) for id_ in ids]
        await self._run_test(engine, table, ids, item)

    @given(data=data(), ids=sets(integers(0, 1000), min_size=10, max_size=100))
    @settings_with_reduced_examples(phases={Phase.generate})
    async def test_many_items(self, *, data: DataObject, ids: set[int]) -> None:
        table = self._make_table()
        engine = await sqlalchemy_engines(data, table)
        await self._run_test(engine, table, ids, [({"id_": id_}, table) for id_ in ids])

    @given(data=data(), id_=integers(0, 10))
    @settings_with_reduced_examples(phases={Phase.generate})
    async def test_mapped_class(self, *, data: DataObject, id_: int) -> None:
        cls = self._make_mapped_class()
        engine = await sqlalchemy_engines(data, cls)
        await self._run_test(engine, cls, {id_}, cls(id_=id_))

    @given(data=data(), ids=sets(integers(0, 10), min_size=1))
    @settings_with_reduced_examples(phases={Phase.generate})
    async def test_mapped_classes(self, *, data: DataObject, ids: set[int]) -> None:
        cls = self._make_mapped_class()
        engine = await sqlalchemy_engines(data, cls)
        await self._run_test(engine, cls, ids, [cls(id_=id_) for id_ in ids])

    @given(data=data(), id_=integers(0, 10))
    @settings_with_reduced_examples(phases={Phase.generate})
    async def test_snake(self, *, data: DataObject, id_: int) -> None:
        table = self._make_table(title=True)
        engine = await sqlalchemy_engines(data, table)
        item = {data.draw(sampled_from(["Id_", "id_"])): id_}, table
        await self._run_test(engine, table, {id_}, item, snake=True)

    @given(data=data(), id_=integers(0, 10))
    @settings_with_reduced_examples(phases={Phase.generate})
    async def test_assume_table_exists(self, *, data: DataObject, id_: int) -> None:
        table = self._make_table()
        engine = await sqlalchemy_engines(data, table)
        with raises(
            (OperationalError, ProgrammingError), match="(no such table|does not exist)"
        ):
            await insert_items(engine, ({"id_": id_}, table), assume_tables_exist=True)

    @given(data=data())
    @settings_with_reduced_examples(phases={Phase.generate})
    async def test_error(self, *, data: DataObject) -> None:
        cls = self._make_mapped_class()
        engine = await sqlalchemy_engines(data, cls)
        with raises(InsertItemsError, match="Item must be valid; got None"):
            await self._run_test(engine, cls, set(), cast("Any", None))

    def _make_table(self, *, title: bool = False) -> Table:
        return Table(
            _table_names(),
            MetaData(),
            Column("Id_" if title else "id_", Integer, primary_key=True),
        )

    def _make_mapped_class(self) -> type[DeclarativeBase]:
        class Base(DeclarativeBase, MappedAsDataclass): ...

        class Example(Base):
            __tablename__ = _table_names()

            id_: Mapped[int] = mapped_column(Integer, kw_only=True, primary_key=True)

        return Example

    async def _run_test(
        self,
        engine: AsyncEngine,
        table_or_orm: TableOrORMInstOrClass,
        ids: set[int],
        /,
        *items: _InsertItem,
        snake: bool = False,
    ) -> None:
        await insert_items(engine, *items, snake=snake)
        sel = select(get_table(table_or_orm).c["Id_" if snake else "id_"])
        async with engine.begin() as conn:
            results = (await conn.execute(sel)).scalars().all()
        assert set(results) == ids


class TestIsPairOfSequenceOfTupleOrStringMappingAndTable:
    @mark.parametrize(
        ("obj", "expected"),
        [
            param(None, False),
            param(([(1, 2, 3)], Table("example", MetaData())), True),
            param(([{"a": 1, "b": 2, "c": 3}], Table("example", MetaData())), True),
            param(
                ([(1, 2, 3), {"a": 1, "b": 2, "c": 3}], Table("example", MetaData())),
                True,
            ),
        ],
    )
    def test_main(self, *, obj: Any, expected: bool) -> None:
        result = _is_pair_of_sequence_of_tuple_or_string_mapping_and_table(obj)
        assert result is expected


class TestIsPairOfStrMappingAndTable:
    @mark.parametrize(
        ("obj", "expected"),
        [
            param(None, False),
            param(((1, 2, 3), Table("example", MetaData())), False),
            param(({"a": 1, "b": 2, "c": 3}, Table("example", MetaData())), True),
        ],
    )
    def test_main(self, *, obj: Any, expected: bool) -> None:
        result = _is_pair_of_str_mapping_and_table(obj)
        assert result is expected


class TestIsPairOfTupleAndTable:
    @mark.parametrize(
        ("obj", "expected"),
        [
            param(None, False),
            param(((1, 2, 3), Table("example", MetaData())), True),
            param(({"a": 1, "b": 2, "c": 3}, Table("example", MetaData())), False),
        ],
    )
    def test_main(self, *, obj: Any, expected: bool) -> None:
        result = _is_pair_of_tuple_and_table(obj)
        assert result is expected


class TestIsPairOfTupleStrMappingAndTable:
    @mark.parametrize(
        ("obj", "expected"),
        [
            param(None, False),
            param(((1, 2, 3), Table("example", MetaData())), True),
            param(({"a": 1, "b": 2, "c": 3}, Table("example", MetaData())), True),
        ],
    )
    def test_main(self, *, obj: Any, expected: bool) -> None:
        result = _is_pair_of_tuple_or_str_mapping_and_table(obj)
        assert result is expected


class TestIsORM:
    def test_orm_inst(self) -> None:
        class Base(DeclarativeBase, MappedAsDataclass): ...

        class Example(Base):
            __tablename__ = "example"

            id_: Mapped[int] = mapped_column(Integer, kw_only=True, primary_key=True)

        obj = Example(id_=1)
        assert is_table_or_orm(obj)

    def test_orm_class(self) -> None:
        class Base(DeclarativeBase, MappedAsDataclass): ...

        class Example(Base):
            __tablename__ = "example"

            id_: Mapped[int] = mapped_column(Integer, kw_only=True, primary_key=True)

        assert is_table_or_orm(Example)

    def test_table(self) -> None:
        table = Table("example", MetaData(), Column("id_", Integer, primary_key=True))
        assert not is_orm(table)

    def test_none(self) -> None:
        assert not is_orm(None)


class TestIsTableOrORM:
    def test_table(self) -> None:
        table = Table("example", MetaData(), Column("id_", Integer, primary_key=True))
        assert is_table_or_orm(table)

    def test_orm_inst(self) -> None:
        class Base(DeclarativeBase, MappedAsDataclass): ...

        class Example(Base):
            __tablename__ = "example"

            id_: Mapped[int] = mapped_column(Integer, kw_only=True, primary_key=True)

        obj = Example(id_=1)
        assert is_table_or_orm(obj)

    def test_orm_class(self) -> None:
        class Base(DeclarativeBase, MappedAsDataclass): ...

        class Example(Base):
            __tablename__ = "example"

            id_: Mapped[int] = mapped_column(Integer, kw_only=True, primary_key=True)

        assert is_table_or_orm(Example)

    def test_other(self) -> None:
        assert not is_table_or_orm(None)


class TestMapMappingToTable:
    @given(id_=integers(0, 10), value=booleans())
    def test_main(self, *, id_: int, value: bool) -> None:
        mapping = {"id_": id_, "value": value}
        table = Table(
            "example",
            MetaData(),
            Column("id_", Integer, primary_key=True),
            Column("value", Boolean),
        )
        result = _map_mapping_to_table(mapping, table)
        assert result == mapping

    @given(data=data(), id_=integers(0, 10), value=booleans())
    def test_snake(self, *, data: DataObject, id_: int, value: bool) -> None:
        mapping = {
            data.draw(sampled_from(["Id_", "id_"])): id_,
            data.draw(sampled_from(["Value", "value"])): value,
        }
        table = Table(
            "example",
            MetaData(),
            Column("id_", Integer, primary_key=True),
            Column("value", Boolean),
        )
        result = _map_mapping_to_table(mapping, table, snake=True)
        expected = {"id_": id_, "value": value}
        assert result == expected

    @given(id_=integers(0, 10), value=booleans(), extra=booleans())
    def test_error_extra_columns(self, *, id_: int, value: bool, extra: bool) -> None:
        mapping = {"id_": id_, "value": value, "extra": extra}
        table = Table(
            "example",
            MetaData(),
            Column("id_", Integer, primary_key=True),
            Column("value", Boolean),
        )
        with raises(
            _MapMappingToTableExtraColumnsError,
            match=r"Mapping .* must be a subset of table columns .*; got extra .*",
        ):
            _ = _map_mapping_to_table(mapping, table)

    @given(id_=integers(0, 10), value=booleans())
    def test_error_snake_empty_error(self, *, id_: int, value: bool) -> None:
        mapping = {"id_": id_, "invalid": value}
        table = Table(
            "example",
            MetaData(),
            Column("id_", Integer, primary_key=True),
            Column("value", Boolean),
        )
        with raises(
            _MapMappingToTableSnakeMapEmptyError,
            match=r"Mapping .* must be a subset of table columns .*; cannot find column to map to 'invalid' modulo snake casing",
        ):
            _ = _map_mapping_to_table(mapping, table, snake=True)

    @given(id_=integers(0, 10), value=booleans())
    def test_error_snake_non_unique_error(self, *, id_: int, value: bool) -> None:
        mapping = {"id_": id_, "value": value}
        table = Table(
            "example",
            MetaData(),
            Column("id_", Integer, primary_key=True),
            Column("value", Boolean),
            Column("Value", Boolean),
        )
        with raises(
            _MapMappingToTableSnakeMapNonUniqueError,
            match=r"Mapping .* must be a subset of table columns .*; found columns 'value', 'Value' and perhaps more to map to 'value' modulo snake casing",
        ):
            _ = _map_mapping_to_table(mapping, table, snake=True)


class TestMigrateData:
    @given(
        data=data(),
        values=lists(
            tuples(integers(0, 10), booleans() | none()),
            min_size=1,
            unique_by=lambda x: x[0],
        ),
    )
    @mark.flaky
    @settings(max_examples=1, phases={Phase.generate})
    async def test_main(
        self, *, data: DataObject, values: list[tuple[int, bool]]
    ) -> None:
        engine1 = await sqlalchemy_engines(data)
        table1 = self._make_table()
        await insert_items(
            engine1, [({"id_": id_, "value": v}, table1) for id_, v in values]
        )
        async with engine1.begin() as conn:
            result1 = (await conn.execute(select(table1))).all()
        assert len(result1) == len(values)

        engine2 = await sqlalchemy_engines(data)
        table2 = self._make_table()
        await migrate_data(table1, engine1, engine2, table_or_orm_to=table2)
        async with engine2.begin() as conn:
            result2 = (await conn.execute(select(table2))).all()
        assert len(result2) == len(values)

    def _make_table(self) -> Table:
        return Table(
            _table_names(),
            MetaData(),
            Column("id_", Integer, primary_key=True),
            Column("value", Boolean, nullable=True),
        )


class TestNormalizeInsertItem:
    @given(case=sampled_from(["tuple", "dict"]), id_=integers(0, 10))
    def test_pair_of_tuple_or_str_mapping_and_table(
        self, *, case: Literal["tuple", "dict"], id_: int
    ) -> None:
        table = self._table
        match case:
            case "tuple":
                item = (id_,), table
            case "dict":
                item = {"id_": id_}, table
        result = one(_normalize_insert_item(item))
        expected = _NormalizedItem(mapping={"id_": id_}, table=table)
        assert result == expected

    @given(case=sampled_from(["tuple", "dict"]), ids=sets(integers(0, 10)))
    def test_pair_of_list_of_tuples_or_str_mappings_and_table(
        self, *, case: Literal["tuple", "dict"], ids: set[int]
    ) -> None:
        table = self._table
        match case:
            case "tuple":
                item = [((id_,)) for id_ in ids], table
            case "dict":
                item = [({"id_": id_}) for id_ in ids], table
        result = list(_normalize_insert_item(item))
        expected = [_NormalizedItem(mapping={"id_": id_}, table=table) for id_ in ids]
        assert result == expected

    @given(case=sampled_from(["tuple", "dict"]), ids=sets(integers()))
    def test_list_of_pairs_of_objs_and_table(
        self, *, case: Literal["tuple", "dict"], ids: set[int]
    ) -> None:
        table = self._table
        match case:
            case "tuple":
                item = [(((id_,), table)) for id_ in ids]
            case "dict":
                item = [({"id_": id_}, table) for id_ in ids]
        result = list(_normalize_insert_item(item))
        expected = [_NormalizedItem(mapping={"id_": id_}, table=table) for id_ in ids]
        assert result == expected

    @given(id_=integers())
    def test_mapped_class(self, *, id_: int) -> None:
        cls = self._mapped_class
        result = one(_normalize_insert_item(cls(id_=id_)))
        expected = _NormalizedItem(mapping={"id_": id_}, table=get_table(cls))
        assert result == expected

    @given(ids=sets(integers(0, 10), min_size=1))
    def test_mapped_classes(self, *, ids: set[int]) -> None:
        cls = self._mapped_class
        result = list(_normalize_insert_item([cls(id_=id_) for id_ in ids]))
        expected = [
            _NormalizedItem(mapping={"id_": id_}, table=get_table(cls)) for id_ in ids
        ]
        assert result == expected

    @given(case=sampled_from(["tuple", "dict"]), id_=integers(0, 10))
    def test_snake(self, *, case: Literal["tuple", "dict"], id_: int) -> None:
        table = Table("example", MetaData(), Column("Id_", Integer, primary_key=True))
        match case:
            case "tuple":
                item = (id_,), table
            case "dict":
                item = {"id_": id_}, table
        result = one(_normalize_insert_item(item, snake=True))
        expected = _NormalizedItem(mapping={"Id_": id_}, table=table)
        assert result == expected

    @mark.parametrize(
        "item",
        [
            param((None,), id="tuple, not pair"),
            param(
                (None, Table("example", MetaData())), id="pair, first element invalid"
            ),
            param(((1, 2, 3), None), id="pair, second element invalid"),
            param([None], id="iterable, invalid"),
            param(None, id="outright invalid"),
        ],
    )
    def test_errors(self, *, item: Any) -> None:
        with raises(_NormalizeInsertItemError, match="Item must be valid; got .*"):
            _ = list(_normalize_insert_item(item))

    @property
    def _table(self) -> Table:
        return Table("example", MetaData(), Column("id_", Integer, primary_key=True))

    @property
    def _mapped_class(self) -> type[DeclarativeBase]:
        class Base(DeclarativeBase, MappedAsDataclass): ...

        class Example(Base):
            __tablename__ = "example"

            id_: Mapped[int] = mapped_column(Integer, kw_only=True, primary_key=True)

        return Example


class TestORMInstToDict:
    @given(id_=integers())
    def test_main(self, *, id_: int) -> None:
        class Base(DeclarativeBase, MappedAsDataclass): ...

        class Example(Base):
            __tablename__ = "example"

            id_: Mapped[int] = mapped_column(Integer, kw_only=True, primary_key=True)

        example = Example(id_=id_)
        result = _orm_inst_to_dict(example)
        expected = {"id_": id_}
        assert result == expected

    @given(id_=integers())
    def test_explicitly_named_column(self, *, id_: int) -> None:
        class Base(DeclarativeBase, MappedAsDataclass): ...

        class Example(Base):
            __tablename__ = "example"

            ID: Mapped[int] = mapped_column(
                Integer, kw_only=True, primary_key=True, name="id"
            )

        example = Example(ID=id_)
        result = _orm_inst_to_dict(example)
        expected = {"id": id_}
        assert result == expected


class TestPrepareInsertOrUpsertItems:
    @given(
        data=data(),
        normalize_item=sampled_from([_normalize_insert_item, _normalize_upsert_item]),
    )
    @settings_with_reduced_examples(phases={Phase.generate})
    async def test_error(
        self, *, data: DataObject, normalize_item: Callable[[Any], Iterator[Any]]
    ) -> None:
        engine = await sqlalchemy_engines(data)
        with raises(
            _PrepareInsertOrUpsertItemsError, match="Item must be valid; got None"
        ):
            _ = _prepare_insert_or_upsert_items(
                normalize_item, engine, cast("Any", None), cast("Any", None)
            )


class TestPrepareInsertOrUpsertItemsMergeItems:
    @given(data=data())
    @settings_with_reduced_examples(phases={Phase.generate})
    async def test_main(self, *, data: DataObject) -> None:
        engine = await sqlalchemy_engines(data)
        table = Table(
            _table_names(),
            MetaData(),
            Column("id_", Integer, primary_key=True),
            Column("value", Boolean, nullable=True),
        )
        await ensure_tables_created(engine, table)
        items = [
            {"id_": 1, "value": True},
            {"id_": 1, "value": False},
            {"id_": 2, "value": False},
            {"id_": 2, "value": True},
        ]
        result = _prepare_insert_or_upsert_items_merge_items(table, items)
        expected = [{"id_": 1, "value": False}, {"id_": 2, "value": True}]
        assert result == expected
        async with engine.begin() as conn:
            _ = await conn.execute(table.insert().values(expected))

    @given(data=data())
    @settings_with_reduced_examples(phases={Phase.generate})
    async def test_just_value(self, *, data: DataObject) -> None:
        engine = await sqlalchemy_engines(data)
        table = Table(
            _table_names(),
            MetaData(),
            Column("id_", Integer, primary_key=True),
            Column("value", Integer),
        )
        await ensure_tables_created(engine, table)
        items = [{"value": 1}, {"value": 2}]
        result = _prepare_insert_or_upsert_items_merge_items(table, items)
        assert result == items
        async with engine.begin() as conn:
            _ = await conn.execute(table.insert().values(items))

    @given(data=data())
    @settings_with_reduced_examples(phases={Phase.generate})
    async def test_autoincrement(self, *, data: DataObject) -> None:
        engine = await sqlalchemy_engines(data)
        table = Table(
            _table_names(),
            MetaData(),
            Column("id_", Integer, primary_key=True, autoincrement=True),
            Column("value", Integer),
        )
        await ensure_tables_created(engine, table)
        items = [{"value": 1}, {"value": 2}]
        result = _prepare_insert_or_upsert_items_merge_items(table, items)
        assert result == items
        async with engine.begin() as conn:
            _ = await conn.execute(table.insert().values(items))


class TestSelectableToString:
    @given(data=data())
    @settings_with_reduced_examples()
    async def test_main(self, *, data: DataObject) -> None:
        engine = await sqlalchemy_engines(data)
        table = Table(
            "example",
            MetaData(),
            Column("id_", Integer, primary_key=True),
            Column("value", Boolean, nullable=True),
        )
        sel = select(table).where(table.c.value >= 1)
        result = selectable_to_string(sel, engine)
        expected = strip_and_dedent(
            """
                SELECT example.id_, example.value --
                FROM example --
                WHERE example.value >= 1
            """.replace("--\n", "\n")
        )
        assert result == expected


class TestTablenameMixin:
    def test_main(self) -> None:
        class Base(DeclarativeBase, MappedAsDataclass, TablenameMixin): ...

        class Example(Base):
            id_: Mapped[int] = mapped_column(Integer, kw_only=True, primary_key=True)

        assert get_table_name(Example) == "example"


class TestTupleToMapping:
    @mark.parametrize(
        ("values", "expected"),
        [
            param((), {}),
            param((1,), {"id_": 1}),
            param((1, True), {"id_": 1, "value": True}),
            param((None, True), {"value": True}),
        ],
        ids=str,
    )
    def test_main(self, *, values: tuple[Any, ...], expected: StrMapping) -> None:
        table = Table(
            "example",
            MetaData(),
            Column("id_", Integer, primary_key=True),
            Column("value", Boolean, nullable=True),
        )
        result = _tuple_to_mapping(values, table)
        assert result == expected


class TestUpserter:
    @given(data=data(), triples=_upsert_lists(nullable=True, min_size=1))
    @mark.flaky
    @settings(max_examples=1, phases={Phase.generate})
    async def test_main(
        self, *, data: DataObject, triples: list[tuple[int, bool, bool]]
    ) -> None:
        table = Table(
            _table_names(),
            MetaData(),
            Column("id_", Integer, primary_key=True),
            Column("value", Boolean, nullable=True),
        )
        engine = await sqlalchemy_engines(data, table)
        pairs = [(id_, init) for id_, init, _ in triples]
        async with Upserter(duration=1.0, sleep_core=0.1, engine=engine) as upserter:
            upserter.put_right_nowait((pairs, table))

        sel = select(table)
        async with engine.begin() as conn:
            res = (await conn.execute(sel)).all()
        assert set(res) == set(pairs)

    @given(data=data())
    async def test_error(self, *, data: DataObject) -> None:
        engine = await sqlalchemy_engines(data)
        upserter = Upserter(engine=engine)
        with raises(UpserterError, match="Error running 'Upserter'"):
            raise UpserterError(upserter=upserter)

    # service

    @given(data=data(), triples=_upsert_lists(nullable=True, min_size=1))
    @mark.flaky
    @settings(max_examples=1, phases={Phase.generate})
    async def test_main_service(
        self, *, data: DataObject, triples: list[tuple[int, bool, bool]]
    ) -> None:
        table = Table(
            _table_names(),
            MetaData(),
            Column("id_", Integer, primary_key=True),
            Column("value", Boolean, nullable=True),
        )
        engine = await sqlalchemy_engines(data, table)
        service = UpsertService(freq=0.1, timeout=1.0, engine=engine)
        pairs = [(id_, init) for id_, init, _ in triples]
        async with service:
            service.put_right_nowait((pairs, table))

        sel = select(table)
        async with engine.begin() as conn:
            res = (await conn.execute(sel)).all()
        assert set(res) == set(pairs)


class TestUpsertItems:
    @given(data=data(), triple=_upsert_triples(nullable=True))
    @settings_with_reduced_examples(phases={Phase.generate})
    async def test_pair_of_dict_and_table(
        self, *, data: DataObject, triple: tuple[int, bool, bool | None]
    ) -> None:
        table = self._make_table()
        engine = await sqlalchemy_engines(data, table)
        id_, init, post = triple
        init_item = {"id_": id_, "value": init}, table
        await self._run_test(engine, table, init_item, expected={(id_, init)})
        post_item = {"id_": id_, "value": post}, table
        _ = await self._run_test(
            engine, table, post_item, expected={(id_, init if post is None else post)}
        )

    @given(
        data=data(),
        triples=_upsert_lists(nullable=True, min_size=1),
        case=sampled_from(["pair-list-of-dicts", "list-of-pair-of-dicts"]),
    )
    @settings_with_reduced_examples(phases={Phase.generate})
    async def test_pair_of_list_of_dicts_and_table(
        self,
        *,
        data: DataObject,
        triples: list[tuple[int, bool, bool | None]],
        case: Literal["pair-list-of-dicts", "list-of-pair-of-dicts"],
    ) -> None:
        table = self._make_table()
        engine = await sqlalchemy_engines(data, table)
        match case:
            case "pair-list-of-dicts":
                init = (
                    [{"id_": id_, "value": init} for id_, init, _ in triples],
                    table,
                )
                post = (
                    [
                        {"id_": id_, "value": post}
                        for id_, _, post in triples
                        if post is not None
                    ],
                    table,
                )
            case "list-of-pair-of-dicts":
                init = [
                    ({"id_": id_, "value": init}, table) for id_, init, _ in triples
                ]
                post = [
                    ({"id_": id_, "value": post}, table)
                    for id_, _, post in triples
                    if post is not None
                ]
        init_expected = {(id_, init) for id_, init, _ in triples}
        _ = await self._run_test(engine, table, init, expected=init_expected)
        post_expected = {
            (id_, init if post is None else post) for id_, init, post in triples
        }
        _ = await self._run_test(engine, table, post, expected=post_expected)

    @given(data=data(), triple=_upsert_triples())
    @settings_with_reduced_examples(phases={Phase.generate})
    async def test_mapped_class(
        self, *, data: DataObject, triple: tuple[int, bool, bool]
    ) -> None:
        cls = self._make_mapped_class()
        engine = await sqlalchemy_engines(data, cls)
        id_, init, post = triple
        _ = await self._run_test(
            engine, cls, cls(id_=id_, value=init), expected={(id_, init)}
        )
        _ = await self._run_test(
            engine, cls, cls(id_=id_, value=post), expected={(id_, post)}
        )

    @given(data=data(), triples=_upsert_lists(nullable=True, min_size=1))
    @settings_with_reduced_examples(phases={Phase.generate})
    async def test_mapped_classes(
        self, *, data: DataObject, triples: list[tuple[int, bool, bool | None]]
    ) -> None:
        cls = self._make_mapped_class()
        engine = await sqlalchemy_engines(data, cls)
        init = [cls(id_=id_, value=init) for id_, init, _ in triples]
        init_expected = {(id_, init) for id_, init, _ in triples}
        _ = await self._run_test(engine, cls, init, expected=init_expected)
        post = [
            cls(id_=id_, value=post) for id_, _, post in triples if post is not None
        ]
        post_expected = {
            (id_, init if post is None else post) for id_, init, post in triples
        }
        _ = await self._run_test(engine, cls, post, expected=post_expected)

    @given(
        data=data(),
        id_=integers(0, 10),
        x_init=booleans(),
        x_post=booleans(),
        y=booleans(),
        selected_or_all=sampled_from(get_args(_SelectedOrAll)),
    )
    @settings_with_reduced_examples(phases={Phase.generate})
    async def test_sel_or_all(
        self,
        *,
        data: DataObject,
        selected_or_all: _SelectedOrAll,
        id_: int,
        x_init: bool,
        x_post: bool,
        y: bool,
    ) -> None:
        table = Table(
            _table_names(),
            MetaData(),
            Column("id_", Integer, primary_key=True),
            Column("x", Boolean, nullable=False),
            Column("y", Boolean, nullable=True),
        )
        engine = await sqlalchemy_engines(data, table)
        _ = await self._run_test(
            engine,
            table,
            ({"id_": id_, "x": x_init, "y": y}, table),
            selected_or_all=selected_or_all,
            expected={(id_, x_init, y)},
        )
        match selected_or_all:
            case "selected":
                expected = (id_, x_post, y)
            case "all":
                expected = (id_, x_post, None)
        _ = await self._run_test(
            engine,
            table,
            ({"id_": id_, "x": x_post}, table),
            selected_or_all=selected_or_all,
            expected={expected},
        )

    @given(data=data(), id_=integers(0, 10))
    @settings_with_reduced_examples(phases={Phase.generate})
    async def test_assume_table_exists(self, *, data: DataObject, id_: int) -> None:
        table = self._make_table()
        engine = await sqlalchemy_engines(data, table)
        with raises((OperationalError, ProgrammingError)):
            await upsert_items(
                engine, ({"id_": id_, "value": True}, table), assume_tables_exist=True
            )

    @given(
        data=data(),
        id1=int32s(),
        id2=int32s(),
        value1=booleans() | none(),
        value2=booleans() | none(),
    )
    @settings_with_reduced_examples(phases={Phase.generate})
    async def test_both_nulls_and_non_nulls(
        self,
        *,
        data: DataObject,
        id1: int,
        id2: int,
        value1: bool | None,
        value2: bool | None,
    ) -> None:
        table = self._make_table()
        engine = await sqlalchemy_engines(data, table)
        _ = assume(id1 != id2)
        item = [{"id_": id1, "value": value1}, {"id_": id2, "value": value2}], table
        await upsert_items(engine, item)

    @given(data=data(), triples=_upsert_lists(nullable=True, min_size=1))
    @settings_with_reduced_examples(phases={Phase.generate})
    async def test_multiple_elements_with_the_same_primary_key(
        self, *, data: DataObject, triples: list[tuple[int, bool, bool | None]]
    ) -> None:
        table = self._make_table()
        engine = await sqlalchemy_engines(data, table)
        pairs = [
            ({"id_": id_, "value": init}, {"id_": id_, "value": post})
            for id_, init, post in triples
        ]
        item = list(chain.from_iterable(pairs)), table
        expected = {
            (id_, init if post is None else post) for id_, init, post in triples
        }
        await self._run_test(engine, table, item, expected=expected)

    @given(data=data())
    @settings_with_reduced_examples(phases={Phase.generate})
    async def test_error(self, *, data: DataObject) -> None:
        table = self._make_table()
        engine = await sqlalchemy_engines(data, table)
        with raises(UpsertItemsError, match="Item must be valid; got None"):
            _ = await self._run_test(engine, table, cast("Any", None))

    def _make_table(self) -> Table:
        return Table(
            _table_names(),
            MetaData(),
            Column("id_", Integer, primary_key=True),
            Column("value", Boolean, nullable=True),
        )

    def _make_mapped_class(self) -> type[DeclarativeBase]:
        class Base(DeclarativeBase, MappedAsDataclass): ...

        class Example(Base):
            __tablename__ = _table_names()

            id_: Mapped[int] = mapped_column(Integer, kw_only=True, primary_key=True)
            value: Mapped[bool] = mapped_column(Boolean, kw_only=True, nullable=False)

        return Example

    async def _run_test(
        self,
        engine: AsyncEngine,
        table_or_orm: TableOrORMInstOrClass,
        /,
        *items: _InsertItem,
        selected_or_all: _SelectedOrAll = "selected",
        expected: set[tuple[Any, ...]] | None = None,
    ) -> None:
        await upsert_items(engine, *items, selected_or_all=selected_or_all)
        sel = select(get_table(table_or_orm))
        async with engine.begin() as conn:
            results = (await conn.execute(sel)).all()
        if expected is not None:
            assert set(results) == expected


class TestUpsertServiceMixin:
    @given(data=data())
    @settings(max_examples=1, phases={Phase.generate})
    async def test_main(self, *, data: DataObject) -> None:
        engine = await sqlalchemy_engines(data)

        @dataclass(kw_only=True)
        class Example(UpsertServiceMixin, Looper[Any]):
            freq: Duration = field(default=_FREQ, repr=False)
            backoff: Duration = field(default=_BACKOFF, repr=False)
            _debug: bool = field(default=True, repr=False)
            upsert_service_database: AsyncEngine = engine
            upsert_service_freq: Duration = field(default=_FREQ, repr=False)
            upsert_service_backoff: Duration = field(default=_BACKOFF, repr=False)

        service = Example(auto_start=True, timeout=1.0)
        async with service:
            ...
        assert_looper_stats(
            service,
            entries=1,
            core_successes=(">=", 40),
            initialization_successes=1,
            stops=1,
        )


class TestYieldPrimaryKeyColumns:
    def test_main(self) -> None:
        table = Table(
            "example",
            MetaData(),
            Column("id1", Integer, primary_key=True),
            Column("id2", Integer, primary_key=True),
            Column("id3", Integer),
        )
        result = list(yield_primary_key_columns(table))
        expected = [
            Column("id1", Integer, primary_key=True),
            Column("id2", Integer, primary_key=True),
        ]
        for c, e in zip(result, expected, strict=True):
            assert c.name == e.name
            assert c.primary_key == e.primary_key

    def test_autoincrement(self) -> None:
        table = Table(
            "example",
            MetaData(),
            Column("id_", Integer, primary_key=True, autoincrement=True),
            Column("x", Integer),
            Column("y", Integer),
        )
        result = list(yield_primary_key_columns(table, autoincrement=False))
        assert result == []
