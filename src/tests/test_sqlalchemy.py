from __future__ import annotations

from enum import Enum, StrEnum, auto
from itertools import chain
from typing import TYPE_CHECKING, Any, Literal, cast, overload, override
from uuid import uuid4

from hypothesis import HealthCheck, Phase, assume, given, settings
from hypothesis.strategies import SearchStrategy, booleans, lists, none, sets, tuples
from pytest import mark, param, raises
from sqlalchemy import Boolean, Column, ForeignKey, Integer, MetaData, Table, select
from sqlalchemy.exc import DatabaseError, OperationalError, ProgrammingError
from sqlalchemy.ext.asyncio import AsyncEngine
from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
    MappedAsDataclass,
    mapped_column,
    relationship,
)

from utilities.hypothesis import int32s, pairs
from utilities.iterables import one
from utilities.modules import is_installed
from utilities.sqlalchemy import (
    CheckEngineError,
    Dialect,
    GetTableError,
    InsertItemsError,
    TablenameMixin,
    TableOrORMInstOrClass,
    UpsertItemsError,
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
    enum_name,
    enum_values,
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
from utilities.typing import get_args, get_literal_elements
from utilities.whenever import format_compact, get_now, to_local_plain

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator
    from pathlib import Path

    from utilities.types import StrMapping


def _table_names() -> str:
    """Generate at unique string."""
    key = str(uuid4()).replace("-", "")
    return f"{format_compact(to_local_plain(get_now()))}_{key}"


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
    async def test_main(self, *, test_engine: AsyncEngine) -> None:
        await check_engine(test_engine)

    async def test_num_tables_pass(self, *, test_engine: AsyncEngine) -> None:
        table = Table(
            _table_names(), MetaData(), Column("id", Integer, primary_key=True)
        )
        await ensure_tables_created(test_engine, table)
        match _get_dialect(test_engine):
            case "sqlite":
                expected = 1
            case "postgresql":
                expected = (int(1e6), 1.0)
            case _ as dialect:
                raise NotImplementedError(dialect)
        await check_engine(test_engine, num_tables=expected)

    async def test_num_tables_error(self, *, test_engine: AsyncEngine) -> None:
        with raises(CheckEngineError, match=r".* must have 100000 table\(s\); got .*"):
            await check_engine(test_engine, num_tables=100000)


class TestColumnwiseMinMax:
    @given(values=sets(pairs(int32s() | none()), min_size=1))
    @settings(
        max_examples=1,
        phases={Phase.generate},
        suppress_health_check={HealthCheck.function_scoped_fixture},
    )
    async def test_main(
        self, *, test_engine: AsyncEngine, values: set[tuple[int | None, int | None]]
    ) -> None:
        table = Table(
            _table_names(),
            MetaData(),
            Column("id_", Integer, primary_key=True, autoincrement=True),
            Column("x", Integer),
            Column("y", Integer),
        )
        await insert_items(test_engine, ([{"x": x, "y": y} for x, y in values], table))
        sel = select(
            table.c["x"],
            table.c["y"],
            columnwise_min(table.c["x"], table.c["y"]).label("min_xy"),
            columnwise_max(table.c["x"], table.c["y"]).label("max_xy"),
        )
        async with test_engine.begin() as conn:
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

    async def test_label(self, *, test_engine: AsyncEngine) -> None:
        table = Table(
            _table_names(),
            MetaData(),
            Column("id_", Integer, primary_key=True, autoincrement=True),
            Column("x", Integer),
        )
        await ensure_tables_created(test_engine, table)
        sel = select(columnwise_min(table.c.x, table.c.x))
        async with test_engine.begin() as conn:
            _ = (await conn.execute(sel)).all()


class TestCreateAsyncEngine:
    def test_async(self, *, tmp_path: Path) -> None:
        engine = create_async_engine("sqlite+aiosqlite", database=tmp_path.name)
        assert isinstance(engine, AsyncEngine)

    def test_query(self, *, tmp_path: Path) -> None:
        engine = create_async_engine(
            "sqlite+aiosqlite",
            database=tmp_path.name,
            query={"arg1": "value1", "arg2": ["value2"]},
        )
        assert isinstance(engine, AsyncEngine)


class TestEnsureTablesCreated:
    async def test_table(self, *, test_engine: AsyncEngine) -> None:
        table = Table(
            _table_names(), MetaData(), Column("id_", Integer, primary_key=True)
        )
        await self._run_test(test_engine, table)

    async def test_mapped_class(self, *, test_engine: AsyncEngine) -> None:
        class Base(DeclarativeBase, MappedAsDataclass): ...

        class Example(Base):
            __tablename__ = _table_names()

            id_: Mapped[int] = mapped_column(Integer, kw_only=True, primary_key=True)

        await self._run_test(test_engine, Example)

    async def _run_test(
        self, engine: AsyncEngine, table_or_orm: TableOrORMInstOrClass, /
    ) -> None:
        for _ in range(2):
            await ensure_tables_created(engine, table_or_orm)
        sel = select(get_table(table_or_orm))
        async with engine.begin() as conn:
            _ = (await conn.execute(sel)).all()


class TestEnsureTablesDropped:
    async def test_table(self, *, test_engine: AsyncEngine) -> None:
        table = Table(
            _table_names(), MetaData(), Column("id_", Integer, primary_key=True)
        )
        await self._run_test(test_engine, table)

    async def test_mapped_class(self, *, test_engine: AsyncEngine) -> None:
        class Base(DeclarativeBase, MappedAsDataclass): ...

        class Example(Base):
            __tablename__ = _table_names()

            id_: Mapped[int] = mapped_column(Integer, kw_only=True, primary_key=True)

        await self._run_test(test_engine, Example)

    async def _run_test(
        self, engine: AsyncEngine, table_or_orm: TableOrORMInstOrClass, /
    ) -> None:
        for _ in range(2):
            await ensure_tables_dropped(engine, table_or_orm)
        sel = select(get_table(table_or_orm))
        with raises(DatabaseError):
            async with engine.begin() as conn:
                _ = await conn.execute(sel)


class TestEnumName:
    def test_main(self) -> None:
        class Example(Enum): ...

        result = enum_name(Example)
        assert result == "example_enum"


class TestEnumValues:
    def test_main(self) -> None:
        class Example(StrEnum):
            true = auto()
            false = auto()

        result = enum_values(Example)
        expected = ["true", "false"]
        assert result == expected


class TestGetChunkSize:
    @mark.parametrize(
        ("num_cols", "chunk_size_frac", "expected"),
        [
            param(2, 1.0, 50),
            param(2, 0.5, 25),
            param(10, 1.0, 10),
            param(10, 0.5, 5),
            param(100, 1.0, 1),
            param(100, 0.5, 1),
        ],
    )
    def test_table(
        self, *, num_cols: int, chunk_size_frac: float, expected: int
    ) -> None:
        table = Table(
            _table_names(),
            MetaData(),
            *[Column(f"id{i}", Integer) for i in range(num_cols)],
        )
        result = get_chunk_size("sqlite", table, chunk_size_frac=chunk_size_frac)
        assert result == expected


class TestGetColumnNames:
    def test_table(self) -> None:
        table = Table(
            _table_names(), MetaData(), Column("id_", Integer, primary_key=True)
        )
        self._run_test(table)

    def test_mapped_class(self) -> None:
        class Base(DeclarativeBase, MappedAsDataclass): ...

        class Example(Base):
            __tablename__ = _table_names()

            id_: Mapped[int] = mapped_column(Integer, kw_only=True, primary_key=True)

        self._run_test(Example)

    def _run_test(self, table_or_orm: TableOrORMInstOrClass, /) -> None:
        assert get_column_names(table_or_orm) == ["id_"]


class TestGetColumns:
    def test_table(self) -> None:
        table = Table(
            _table_names(), MetaData(), Column("id", Integer, primary_key=True)
        )
        self._run_test(table)

    def test_mapped_class(self) -> None:
        class Base(DeclarativeBase, MappedAsDataclass): ...

        class Example(Base):
            __tablename__ = _table_names()

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
    @mark.parametrize("dialect", get_args(Dialect))
    def test_max_params(self, *, dialect: Dialect) -> None:
        max_params = _get_dialect_max_params(dialect)
        assert isinstance(max_params, int)


class TestGetPrimaryKeyValues:
    @given(id1=int32s(), id2=int32s(), value=booleans())
    def test_main(self, *, id1: int, id2: int, value: bool) -> None:
        class Base(DeclarativeBase, MappedAsDataclass): ...

        class Example(Base):
            __tablename__ = _table_names()

            id1: Mapped[int] = mapped_column(Integer, kw_only=True, primary_key=True)
            id2: Mapped[int] = mapped_column(Integer, kw_only=True, primary_key=True)
            value: Mapped[bool] = mapped_column(Boolean, kw_only=True, nullable=False)

        obj = Example(id1=id1, id2=id2, value=value)
        result = get_primary_key_values(obj)
        expected = (id1, id2)
        assert result == expected


class TestGetTable:
    def test_table(self) -> None:
        table = Table(
            _table_names(), MetaData(), Column("id_", Integer, primary_key=True)
        )
        result = get_table(table)
        assert result is table

    def test_mapped_class(self) -> None:
        class Base(DeclarativeBase, MappedAsDataclass): ...

        class Example(Base):
            __tablename__ = _table_names()

            id: Mapped[int] = mapped_column(Integer, kw_only=True, primary_key=True)

        result = get_table(Example)
        expected = Example.__table__
        assert result is expected

    def test_instance_of_mapped_class(self) -> None:
        class Base(DeclarativeBase, MappedAsDataclass): ...

        class Example(Base):
            __tablename__ = _table_names()

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
        name = _table_names()
        table = Table(name, MetaData(), Column("id_", Integer, primary_key=True))
        result = get_table_name(table)
        assert result == name

    def test_mapped_class(self) -> None:
        name = _table_names()

        class Base(DeclarativeBase, MappedAsDataclass): ...

        class Example(Base):
            __tablename__ = name

            id_: Mapped[int] = mapped_column(Integer, kw_only=True, primary_key=True)

        result = get_table_name(Example)
        assert result == name


class TestHashPrimaryKeyValues:
    @given(id1=int32s(), id2=int32s(), value=booleans())
    def test_main(self, *, id1: int, id2: int, value: bool) -> None:
        class Base(DeclarativeBase, MappedAsDataclass): ...

        class Example(Base):
            __tablename__ = _table_names()

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
    @given(id_=int32s())
    @mark.parametrize("case", [param("tuple"), param("dict")])
    @settings(
        max_examples=1,
        phases={Phase.generate},
        suppress_health_check={HealthCheck.function_scoped_fixture},
    )
    async def test_pair_of_obj_and_table(
        self, *, case: Literal["tuple", "dict"], id_: int, test_engine: AsyncEngine
    ) -> None:
        table = self._make_table()
        match case:
            case "tuple":
                item = (id_,), table
            case "dict":
                item = {"id_": id_}, table
        await self._run_test(test_engine, table, {id_}, item)

    @given(ids=sets(int32s(), min_size=1))
    @mark.parametrize(
        "case",
        [
            param("pair-list-of-dicts"),
            param("pair-list-of-tuples"),
            param("list-of-pair-of-dicts"),
            param("list-of-pair-of-tuples"),
        ],
    )
    @settings(
        max_examples=1,
        phases={Phase.generate},
        suppress_health_check={HealthCheck.function_scoped_fixture},
    )
    async def test_pair_of_objs_and_table_or_list_of_pairs_of_objs_and_table(
        self,
        *,
        case: Literal[
            "pair-list-of-tuples",
            "pair-list-of-dicts",
            "list-of-pair-of-tuples",
            "list-of-pair-of-dicts",
        ],
        ids: set[int],
        test_engine: AsyncEngine,
    ) -> None:
        table = self._make_table()
        match case:
            case "pair-list-of-tuples":
                item = [((id_,)) for id_ in ids], table
            case "pair-list-of-dicts":
                item = [({"id_": id_}) for id_ in ids], table
            case "list-of-pair-of-tuples":
                item = [((id_,), table) for id_ in ids]
            case "list-of-pair-of-dicts":
                item = [({"id_": id_}, table) for id_ in ids]
        await self._run_test(test_engine, table, ids, item)

    @given(ids=sets(int32s(), min_size=10, max_size=100))
    @settings(
        max_examples=1,
        phases={Phase.generate},
        suppress_health_check={HealthCheck.function_scoped_fixture},
    )
    async def test_many_items(self, *, ids: set[int], test_engine: AsyncEngine) -> None:
        table = self._make_table()
        await self._run_test(
            test_engine, table, ids, [({"id_": id_}, table) for id_ in ids]
        )

    @given(id_=int32s())
    @settings(
        max_examples=1,
        phases={Phase.generate},
        suppress_health_check={HealthCheck.function_scoped_fixture},
    )
    async def test_mapped_class(self, *, id_: int, test_engine: AsyncEngine) -> None:
        cls = self._make_mapped_class()
        await self._run_test(test_engine, cls, {id_}, cls(id_=id_))

    @given(ids=sets(int32s(), min_size=1))
    @settings(
        max_examples=1,
        phases={Phase.generate},
        suppress_health_check={HealthCheck.function_scoped_fixture},
    )
    async def test_mapped_classes(
        self, *, ids: set[int], test_engine: AsyncEngine
    ) -> None:
        cls = self._make_mapped_class()
        await self._run_test(test_engine, cls, ids, [cls(id_=id_) for id_ in ids])

    @given(id_=int32s())
    @mark.parametrize("key", [param("Id_"), param("id_")])
    @settings(
        max_examples=1,
        phases={Phase.generate},
        suppress_health_check={HealthCheck.function_scoped_fixture},
    )
    async def test_snake(self, *, id_: int, key: str, test_engine: AsyncEngine) -> None:
        table = self._make_table(title=True)
        item = {key: id_}, table
        await self._run_test(test_engine, table, {id_}, item, snake=True)

    @given(id_=int32s())
    @settings(
        max_examples=1,
        phases={Phase.generate},
        suppress_health_check={HealthCheck.function_scoped_fixture},
    )
    async def test_assume_table_exists(
        self, *, id_: int, test_engine: AsyncEngine
    ) -> None:
        table = self._make_table()
        with raises(
            (OperationalError, ProgrammingError), match="(no such table|does not exist)"
        ):
            await insert_items(
                test_engine, ({"id_": id_}, table), assume_tables_exist=True
            )

    async def test_error(self, *, test_engine: AsyncEngine) -> None:
        cls = self._make_mapped_class()
        with raises(InsertItemsError, match="Item must be valid; got None"):
            await self._run_test(test_engine, cls, set(), cast("Any", None))

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
            param(([(1, 2, 3)], Table(_table_names(), MetaData())), True),
            param(
                ([{"a": 1, "b": 2, "c": 3}], Table(_table_names(), MetaData())), True
            ),
            param(
                (
                    [(1, 2, 3), {"a": 1, "b": 2, "c": 3}],
                    Table(_table_names(), MetaData()),
                ),
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
            param(((1, 2, 3), Table(_table_names(), MetaData())), False),
            param(({"a": 1, "b": 2, "c": 3}, Table(_table_names(), MetaData())), True),
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
            param(((1, 2, 3), Table(_table_names(), MetaData())), True),
            param(({"a": 1, "b": 2, "c": 3}, Table(_table_names(), MetaData())), False),
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
            param(((1, 2, 3), Table(_table_names(), MetaData())), True),
            param(({"a": 1, "b": 2, "c": 3}, Table(_table_names(), MetaData())), True),
        ],
    )
    def test_main(self, *, obj: Any, expected: bool) -> None:
        result = _is_pair_of_tuple_or_str_mapping_and_table(obj)
        assert result is expected


class TestIsORM:
    def test_orm_inst(self) -> None:
        class Base(DeclarativeBase, MappedAsDataclass): ...

        class Example(Base):
            __tablename__ = _table_names()

            id_: Mapped[int] = mapped_column(Integer, kw_only=True, primary_key=True)

        obj = Example(id_=1)
        assert is_table_or_orm(obj)

    def test_orm_class(self) -> None:
        class Base(DeclarativeBase, MappedAsDataclass): ...

        class Example(Base):
            __tablename__ = _table_names()

            id_: Mapped[int] = mapped_column(Integer, kw_only=True, primary_key=True)

        assert is_table_or_orm(Example)

    def test_table(self) -> None:
        table = Table(
            _table_names(), MetaData(), Column("id_", Integer, primary_key=True)
        )
        assert not is_orm(table)

    def test_none(self) -> None:
        assert not is_orm(None)


class TestIsTableOrORM:
    def test_table(self) -> None:
        table = Table(
            _table_names(), MetaData(), Column("id_", Integer, primary_key=True)
        )
        assert is_table_or_orm(table)

    def test_orm_inst(self) -> None:
        class Base(DeclarativeBase, MappedAsDataclass): ...

        class Example(Base):
            __tablename__ = _table_names()

            id_: Mapped[int] = mapped_column(Integer, kw_only=True, primary_key=True)

        obj = Example(id_=1)
        assert is_table_or_orm(obj)

    def test_orm_class(self) -> None:
        class Base(DeclarativeBase, MappedAsDataclass): ...

        class Example(Base):
            __tablename__ = _table_names()

            id_: Mapped[int] = mapped_column(Integer, kw_only=True, primary_key=True)

        assert is_table_or_orm(Example)

    def test_other(self) -> None:
        assert not is_table_or_orm(None)


class TestMapMappingToTable:
    @given(id_=int32s(), value=booleans())
    def test_main(self, *, id_: int, value: bool) -> None:
        mapping = {"id_": id_, "value": value}
        table = Table(
            _table_names(),
            MetaData(),
            Column("id_", Integer, primary_key=True),
            Column("value", Boolean),
        )
        result = _map_mapping_to_table(mapping, table)
        assert result == mapping

    @given(id_=int32s(), value=booleans())
    @mark.parametrize("key1", [param("Id_"), param("id_")])
    @mark.parametrize("key2", [param("Value"), param("value")])
    def test_snake(self, *, key1: str, id_: int, key2: str, value: bool) -> None:
        mapping = {key1: id_, key2: value}
        table = Table(
            _table_names(),
            MetaData(),
            Column("id_", Integer, primary_key=True),
            Column("value", Boolean),
        )
        result = _map_mapping_to_table(mapping, table, snake=True)
        expected = {"id_": id_, "value": value}
        assert result == expected

    @given(id_=int32s(), value=booleans(), extra=booleans())
    def test_error_extra_columns(self, *, id_: int, value: bool, extra: bool) -> None:
        mapping = {"id_": id_, "value": value, "extra": extra}
        table = Table(
            _table_names(),
            MetaData(),
            Column("id_", Integer, primary_key=True),
            Column("value", Boolean),
        )
        with raises(
            _MapMappingToTableExtraColumnsError,
            match=r"Mapping .* must be a subset of table columns .*; got extra .*",
        ):
            _ = _map_mapping_to_table(mapping, table)

    @given(id_=int32s(), value=booleans())
    def test_error_snake_empty_error(self, *, id_: int, value: bool) -> None:
        mapping = {"id_": id_, "invalid": value}
        table = Table(
            _table_names(),
            MetaData(),
            Column("id_", Integer, primary_key=True),
            Column("value", Boolean),
        )
        with raises(
            _MapMappingToTableSnakeMapEmptyError,
            match=r"Mapping .* must be a subset of table columns .*; cannot find column to map to 'invalid' modulo snake casing",
        ):
            _ = _map_mapping_to_table(mapping, table, snake=True)

    @given(id_=int32s(), value=booleans())
    def test_error_snake_non_unique_error(self, *, id_: int, value: bool) -> None:
        mapping = {"id_": id_, "value": value}
        table = Table(
            _table_names(),
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
        values=lists(
            tuples(int32s(), booleans() | none()), min_size=1, unique_by=lambda x: x[0]
        )
    )
    @settings(
        max_examples=1,
        phases={Phase.generate},
        suppress_health_check={HealthCheck.function_scoped_fixture},
    )
    async def test_main(
        self, *, values: list[tuple[int, bool]], test_engine: AsyncEngine
    ) -> None:
        table1 = self._make_table()
        await insert_items(
            test_engine, [({"id_": id_, "value": v}, table1) for id_, v in values]
        )
        async with test_engine.begin() as conn:
            result1 = (await conn.execute(select(table1))).all()
        assert len(result1) == len(values)

        table2 = self._make_table()
        await migrate_data(table1, test_engine, test_engine, table_or_orm_to=table2)
        async with test_engine.begin() as conn:
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
    @given(id_=int32s())
    @mark.parametrize("case", [param("tuple"), param("dict")])
    @settings(
        max_examples=1,
        phases={Phase.generate},
        suppress_health_check={HealthCheck.function_scoped_fixture},
    )
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

    @given(ids=sets(int32s()))
    @mark.parametrize("case", [param("tuple"), param("dict")])
    @settings(
        max_examples=1,
        phases={Phase.generate},
        suppress_health_check={HealthCheck.function_scoped_fixture},
    )
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

    @given(ids=sets(int32s()))
    @mark.parametrize("case", [param("tuple"), param("dict")])
    @settings(
        max_examples=1,
        phases={Phase.generate},
        suppress_health_check={HealthCheck.function_scoped_fixture},
    )
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

    @given(id_=int32s())
    def test_mapped_class(self, *, id_: int) -> None:
        cls = self._mapped_class
        result = one(_normalize_insert_item(cls(id_=id_)))
        expected = _NormalizedItem(mapping={"id_": id_}, table=get_table(cls))
        assert result == expected

    @given(ids=sets(int32s(), min_size=1))
    def test_mapped_classes(self, *, ids: set[int]) -> None:
        cls = self._mapped_class
        result = list(_normalize_insert_item([cls(id_=id_) for id_ in ids]))
        expected = [
            _NormalizedItem(mapping={"id_": id_}, table=get_table(cls)) for id_ in ids
        ]
        assert result == expected

    @given(id_=int32s())
    @mark.parametrize("case", [param("tuple"), param("dict")])
    @settings(
        max_examples=1,
        phases={Phase.generate},
        suppress_health_check={HealthCheck.function_scoped_fixture},
    )
    def test_snake(self, *, case: Literal["tuple", "dict"], id_: int) -> None:
        table = Table(
            _table_names(), MetaData(), Column("Id_", Integer, primary_key=True)
        )
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
                (None, Table(_table_names(), MetaData())),
                id="pair, first element invalid",
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
        return Table(
            _table_names(), MetaData(), Column("id_", Integer, primary_key=True)
        )

    @property
    def _mapped_class(self) -> type[DeclarativeBase]:
        class Base(DeclarativeBase, MappedAsDataclass): ...

        class Example(Base):
            __tablename__ = _table_names()

            id_: Mapped[int] = mapped_column(Integer, kw_only=True, primary_key=True)

        return Example


class TestORMInstToDict:
    @given(id_=int32s())
    def test_main(self, *, id_: int) -> None:
        class Base(DeclarativeBase, MappedAsDataclass): ...

        class Example(Base):
            __tablename__ = _table_names()

            id_: Mapped[int] = mapped_column(Integer, kw_only=True, primary_key=True)

        example = Example(id_=id_)
        result = _orm_inst_to_dict(example)
        expected = {"id_": id_}
        assert result == expected

    @given(id_=int32s())
    def test_explicitly_named_column(self, *, id_: int) -> None:
        class Base(DeclarativeBase, MappedAsDataclass): ...

        class Example(Base):
            __tablename__ = _table_names()

            ID: Mapped[int] = mapped_column(
                Integer, kw_only=True, primary_key=True, name="id"
            )

        example = Example(ID=id_)
        result = _orm_inst_to_dict(example)
        expected = {"id": id_}
        assert result == expected

    @given(parent_id=int32s(), child_id=int32s())
    def test_relationship(self, *, parent_id: int, child_id: int) -> None:
        class Base(DeclarativeBase, MappedAsDataclass): ...

        class Parent(Base):
            __tablename__ = _table_names()

            id_: Mapped[int] = mapped_column(Integer, kw_only=True, primary_key=True)

            children: Mapped[list[Child]] = relationship(
                "Child", init=False, back_populates="parent"
            )

        class Child(Base):
            __tablename__ = _table_names()

            id_: Mapped[int] = mapped_column(Integer, kw_only=True, primary_key=True)
            parent_id: Mapped[int] = mapped_column(
                ForeignKey(f"{Parent.__tablename__}.id_"), kw_only=True, nullable=False
            )

            parent: Mapped[Parent] = relationship(
                "Parent", init=False, back_populates="children"
            )

        parent = Parent(id_=parent_id)
        result = _orm_inst_to_dict(parent)
        expected = {"id_": parent_id}
        assert result == expected

        child = Child(id_=child_id, parent_id=parent_id)
        result = _orm_inst_to_dict(child)
        expected = {"id_": child_id, "parent_id": parent_id}
        assert result == expected


class TestPrepareInsertOrUpsertItems:
    @mark.parametrize(
        "normalize_item", [param(_normalize_insert_item), param(_normalize_upsert_item)]
    )
    async def test_error(
        self,
        *,
        normalize_item: Callable[[Any], Iterator[Any]],
        test_engine: AsyncEngine,
    ) -> None:
        with raises(
            _PrepareInsertOrUpsertItemsError, match="Item must be valid; got None"
        ):
            _ = _prepare_insert_or_upsert_items(
                normalize_item, test_engine, cast("Any", None), cast("Any", None)
            )


class TestPrepareInsertOrUpsertItemsMergeItems:
    async def test_main(self, *, test_engine: AsyncEngine) -> None:
        table = Table(
            _table_names(),
            MetaData(),
            Column("id_", Integer, primary_key=True),
            Column("value", Boolean, nullable=True),
        )
        await ensure_tables_created(test_engine, table)
        items = [
            {"id_": 1, "value": True},
            {"id_": 1, "value": False},
            {"id_": 2, "value": False},
            {"id_": 2, "value": True},
        ]
        result = _prepare_insert_or_upsert_items_merge_items(table, items)
        expected = [{"id_": 1, "value": False}, {"id_": 2, "value": True}]
        assert result == expected
        async with test_engine.begin() as conn:
            _ = await conn.execute(table.insert().values(expected))

    async def test_just_value(self, *, test_engine: AsyncEngine) -> None:
        table = Table(
            _table_names(),
            MetaData(),
            Column("id_", Integer, primary_key=True),
            Column("value", Integer),
        )
        await ensure_tables_created(test_engine, table)
        items = [{"value": 1}, {"value": 2}]
        result = _prepare_insert_or_upsert_items_merge_items(table, items)
        assert result == items
        async with test_engine.begin() as conn:
            _ = await conn.execute(table.insert().values(items))

    async def test_autoincrement(self, *, test_engine: AsyncEngine) -> None:
        table = Table(
            _table_names(),
            MetaData(),
            Column("id_", Integer, primary_key=True, autoincrement=True),
            Column("value", Integer),
        )
        await ensure_tables_created(test_engine, table)
        items = [{"value": 1}, {"value": 2}]
        result = _prepare_insert_or_upsert_items_merge_items(table, items)
        assert result == items
        async with test_engine.begin() as conn:
            _ = await conn.execute(table.insert().values(items))


class TestSelectableToString:
    async def test_main(self, *, test_engine: AsyncEngine) -> None:
        table = Table(
            "example",
            MetaData(),
            Column("id_", Integer, primary_key=True),
            Column("value", Boolean, nullable=True),
        )
        sel = select(table).where(table.c.value >= 1)
        result = selectable_to_string(sel, test_engine)
        expected = strip_and_dedent(
            """
                SELECT example.id_, example.value *
                FROM example *
                WHERE example.value >= 1
            """.replace("*", "")
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
            _table_names(),
            MetaData(),
            Column("id_", Integer, primary_key=True),
            Column("value", Boolean, nullable=True),
        )
        result = _tuple_to_mapping(values, table)
        assert result == expected


class TestUpsertItems:
    @given(triple=_upsert_triples(nullable=True))
    @settings(
        max_examples=1,
        phases={Phase.generate},
        suppress_health_check={HealthCheck.function_scoped_fixture},
    )
    async def test_pair_of_dict_and_table(
        self, *, triple: tuple[int, bool, bool | None], test_engine: AsyncEngine
    ) -> None:
        table = self._make_table()
        id_, init, post = triple
        init_item = {"id_": id_, "value": init}, table
        await self._run_test(test_engine, table, init_item, expected={(id_, init)})
        post_item = {"id_": id_, "value": post}, table
        _ = await self._run_test(
            test_engine,
            table,
            post_item,
            expected={(id_, init if post is None else post)},
        )

    @given(triples=_upsert_lists(nullable=True, min_size=1))
    @mark.parametrize(
        "case", [param("pair-list-of-dicts"), param("list-of-pair-of-dicts")]
    )
    @settings(
        max_examples=1,
        phases={Phase.generate},
        suppress_health_check={HealthCheck.function_scoped_fixture},
    )
    async def test_pair_of_list_of_dicts_and_table(
        self,
        *,
        triples: list[tuple[int, bool, bool | None]],
        case: Literal["pair-list-of-dicts", "list-of-pair-of-dicts"],
        test_engine: AsyncEngine,
    ) -> None:
        table = self._make_table()
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
        _ = await self._run_test(test_engine, table, init, expected=init_expected)
        post_expected = {
            (id_, init if post is None else post) for id_, init, post in triples
        }
        _ = await self._run_test(test_engine, table, post, expected=post_expected)

    @given(triple=_upsert_triples())
    @settings(
        max_examples=1,
        phases={Phase.generate},
        suppress_health_check={HealthCheck.function_scoped_fixture},
    )
    async def test_mapped_class(
        self, *, triple: tuple[int, bool, bool], test_engine: AsyncEngine
    ) -> None:
        cls = self._make_mapped_class()
        id_, init, post = triple
        _ = await self._run_test(
            test_engine, cls, cls(id_=id_, value=init), expected={(id_, init)}
        )
        _ = await self._run_test(
            test_engine, cls, cls(id_=id_, value=post), expected={(id_, post)}
        )

    @given(parent=_upsert_triples(), child=_upsert_triples())
    @settings(
        max_examples=1,
        phases={Phase.generate},
        suppress_health_check={HealthCheck.function_scoped_fixture},
    )
    async def test_mapped_class_with_rel(
        self,
        *,
        parent: tuple[int, bool, bool],
        child: tuple[int, bool, bool],
        test_engine: AsyncEngine,
    ) -> None:
        class Base(DeclarativeBase, MappedAsDataclass):
            pass

        class Parent(Base):
            __tablename__ = _table_names()

            id_: Mapped[int] = mapped_column(Integer, kw_only=True, primary_key=True)
            value: Mapped[bool] = mapped_column(Boolean, kw_only=True, nullable=False)

            children: Mapped[list[Child]] = relationship(
                "Child", init=False, back_populates="parent"
            )

        class Child(Base):
            __tablename__ = _table_names()

            id_: Mapped[int] = mapped_column(Integer, kw_only=True, primary_key=True)
            parent_id: Mapped[int] = mapped_column(
                ForeignKey(f"{Parent.__tablename__}.id_"), kw_only=True, nullable=False
            )
            value: Mapped[bool] = mapped_column(Boolean, kw_only=True, nullable=False)

            parent: Mapped[Parent] = relationship(
                "Parent", init=False, back_populates="children"
            )

        parent_id, parent_init, _ = parent
        child_id, child_init, child_post = child
        await self._run_test(
            test_engine,
            Parent,
            Parent(id_=parent_id, value=parent_init),
            expected={(parent_id, parent_init)},
        )
        await self._run_test(
            test_engine,
            Child,
            Child(id_=child_id, parent_id=parent_id, value=child_init),
            expected={(child_id, parent_id, child_init)},
        )
        await self._run_test(
            test_engine,
            Child,
            Child(id_=child_id, parent_id=parent_id, value=child_post),
            expected={(child_id, parent_id, child_post)},
        )

    @given(triples=_upsert_lists(nullable=True, min_size=1))
    @settings(
        max_examples=1,
        phases={Phase.generate},
        suppress_health_check={HealthCheck.function_scoped_fixture},
    )
    async def test_mapped_classes(
        self, *, triples: list[tuple[int, bool, bool | None]], test_engine: AsyncEngine
    ) -> None:
        cls = self._make_mapped_class()
        init = [cls(id_=id_, value=init) for id_, init, _ in triples]
        init_expected = {(id_, init) for id_, init, _ in triples}
        _ = await self._run_test(test_engine, cls, init, expected=init_expected)
        post = [
            cls(id_=id_, value=post) for id_, _, post in triples if post is not None
        ]
        post_expected = {
            (id_, init if post is None else post) for id_, init, post in triples
        }
        _ = await self._run_test(test_engine, cls, post, expected=post_expected)

    @given(id_=int32s(), x_init=booleans(), x_post=booleans(), y=booleans())
    @mark.parametrize("selected_or_all", get_literal_elements(_SelectedOrAll))
    @settings(
        max_examples=1,
        phases={Phase.generate},
        suppress_health_check={HealthCheck.function_scoped_fixture},
    )
    async def test_sel_or_all(
        self,
        *,
        selected_or_all: _SelectedOrAll,
        id_: int,
        x_init: bool,
        x_post: bool,
        y: bool,
        test_engine: AsyncEngine,
    ) -> None:
        table = Table(
            _table_names(),
            MetaData(),
            Column("id_", Integer, primary_key=True),
            Column("x", Boolean, nullable=False),
            Column("y", Boolean, nullable=True),
        )
        _ = await self._run_test(
            test_engine,
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
            test_engine,
            table,
            ({"id_": id_, "x": x_post}, table),
            selected_or_all=selected_or_all,
            expected={expected},
        )

    @given(id_=int32s())
    @settings(
        max_examples=1,
        phases={Phase.generate},
        suppress_health_check={HealthCheck.function_scoped_fixture},
    )
    async def test_assume_table_exists(
        self, *, id_: int, test_engine: AsyncEngine
    ) -> None:
        table = self._make_table()
        with raises((OperationalError, ProgrammingError)):
            await upsert_items(
                test_engine,
                ({"id_": id_, "value": True}, table),
                assume_tables_exist=True,
            )

    @given(
        id1=int32s(),
        id2=int32s(),
        value1=booleans() | none(),
        value2=booleans() | none(),
    )
    @settings(
        max_examples=1,
        phases={Phase.generate},
        suppress_health_check={HealthCheck.function_scoped_fixture},
    )
    async def test_both_nulls_and_non_nulls(
        self,
        *,
        id1: int,
        id2: int,
        value1: bool | None,
        value2: bool | None,
        test_engine: AsyncEngine,
    ) -> None:
        table = self._make_table()
        _ = assume(id1 != id2)
        item = [{"id_": id1, "value": value1}, {"id_": id2, "value": value2}], table
        await upsert_items(test_engine, item)

    @given(triples=_upsert_lists(nullable=True, min_size=1))
    @settings(
        max_examples=1,
        phases={Phase.generate},
        suppress_health_check={HealthCheck.function_scoped_fixture},
    )
    async def test_multiple_elements_with_the_same_primary_key(
        self, *, triples: list[tuple[int, bool, bool | None]], test_engine: AsyncEngine
    ) -> None:
        table = self._make_table()
        pairs = [
            ({"id_": id_, "value": init}, {"id_": id_, "value": post})
            for id_, init, post in triples
        ]
        item = list(chain.from_iterable(pairs)), table
        expected = {
            (id_, init if post is None else post) for id_, init, post in triples
        }
        await self._run_test(test_engine, table, item, expected=expected)

    async def test_error(self, *, test_engine: AsyncEngine) -> None:
        table = self._make_table()
        with raises(UpsertItemsError, match="Item must be valid; got None"):
            _ = await self._run_test(test_engine, table, cast("Any", None))

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


class TestYieldPrimaryKeyColumns:
    def test_main(self) -> None:
        table = Table(
            _table_names(),
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
            _table_names(),
            MetaData(),
            Column("id_", Integer, primary_key=True, autoincrement=True),
            Column("x", Integer),
            Column("y", Integer),
        )
        result = list(yield_primary_key_columns(table, autoincrement=False))
        assert result == []
