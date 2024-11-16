from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any, Literal, cast, overload

from hypothesis import given, settings
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
    uuids,
)
from pytest import mark, param, raises
from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Engine,
    Integer,
    MetaData,
    Row,
    Select,
    Table,
    func,
    select,
)
from sqlalchemy.exc import DatabaseError, OperationalError, ProgrammingError
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine
from sqlalchemy.orm import DeclarativeBase, Mapped, MappedAsDataclass, mapped_column

from tests.conftest import SKIPIF_CI
from utilities.hashlib import md5_hash
from utilities.hypothesis import (
    int32s,
    sets_fixed_length,
    sqlalchemy_engines,
    temp_paths,
)
from utilities.iterables import one
from utilities.modules import is_installed
from utilities.sqlalchemy import (
    AsyncEngineOrConnection,
    Dialect,
    GetTableError,
    InsertItemsError,
    TablenameMixin,
    TableOrMappedClass,
    _get_dialect,
    _get_dialect_max_params,
    _InsertItem,
    _is_insert_item_pair,
    _is_upsert_item_pair,
    _normalize_insert_item,
    _normalize_upsert_item,
    _NormalizedInsertItem,
    _NormalizedUpsertItem,
    _NormalizeInsertItemError,
    _NormalizeUpsertItemError,
    _prepare_insert_or_upsert_items,
    _PrepareInsertOrUpsertItemsError,
    _UpsertItem,
    columnwise_max,
    columnwise_min,
    create_async_engine,
    ensure_tables_created,
    ensure_tables_dropped,
    get_chunk_size,
    get_column_names,
    get_columns,
    get_table,
    get_table_name,
    insert_items,
    is_mapped_class,
    is_table_or_mapped_class,
    mapped_class_to_dict,
    selectable_to_string,
    serialize_engine,
    upsert_items,
    yield_connection,
    yield_primary_key_columns,
)
from utilities.text import strip_and_dedent
from utilities.typing import get_args

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator, Sequence
    from pathlib import Path
    from uuid import UUID

    from utilities.asyncio import Coroutine1


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
    return tuples(integers(0, 10), booleans(), elements)


def _upsert_lists(
    *, nullable: bool = False, min_size: int = 0, max_size: int | None = None
) -> SearchStrategy[list[tuple[int, bool, bool | None]]]:
    return lists(
        _upsert_triples(nullable=nullable),
        min_size=min_size,
        max_size=max_size,
        unique_by=lambda x: x[0],
    )


class TestColumnwiseMinMax:
    @given(
        data=data(),
        name=uuids(),
        values=sets(
            tuples(integers(0, 10) | none(), integers(0, 10) | none()), min_size=1
        ),
    )
    async def test_main(
        self,
        *,
        data: DataObject,
        name: UUID,
        values: set[tuple[int | None, int | None]],
    ) -> None:
        table = Table(
            f"test_{name}",
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

    @given(data=data(), name=uuids())
    async def test_label(self, *, data: DataObject, name: UUID) -> None:
        table = Table(
            f"test_{name}",
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
    def test_async(self, *, temp_path: Path) -> None:
        engine = create_async_engine("sqlite+aiosqlite", database=temp_path.name)
        assert isinstance(engine, AsyncEngine)

    @given(temp_path=temp_paths())
    def test_query(self, *, temp_path: Path) -> None:
        engine = create_async_engine(
            "sqlite",
            database=temp_path.name,
            query={"arg1": "value1", "arg2": ["value2"]},
        )
        assert isinstance(engine, Engine)


class TestEnsureTablesCreated:
    @given(data=data(), name=uuids())
    async def test_table(self, *, data: DataObject, name: UUID) -> None:
        table = Table(
            f"test_{name}", MetaData(), Column("id_", Integer, primary_key=True)
        )
        engine = await sqlalchemy_engines(data, table)
        await self._run_test(engine, table)

    @given(data=data(), name=uuids())
    async def test_mapped_class(self, *, data: DataObject, name: UUID) -> None:
        class Base(DeclarativeBase, MappedAsDataclass): ...

        class Example(Base):
            __tablename__ = f"test_{name}"

            id_: Mapped[int] = mapped_column(Integer, kw_only=True, primary_key=True)

        engine = await sqlalchemy_engines(data, Example)
        await self._run_test(engine, Example)

    async def _run_test(
        self,
        engine_or_conn: AsyncEngineOrConnection,
        table_or_mapped_class: TableOrMappedClass,
        /,
    ) -> None:
        for _ in range(2):
            await ensure_tables_created(engine_or_conn, table_or_mapped_class)
        sel = select(get_table(table_or_mapped_class))
        async with yield_connection(engine_or_conn) as conn:
            _ = (await conn.execute(sel)).all()


class TestEnsureTablesDropped:
    @given(data=data(), name=uuids())
    async def test_table(self, *, data: DataObject, name: UUID) -> None:
        table = Table(
            f"test_{name}", MetaData(), Column("id_", Integer, primary_key=True)
        )
        engine = await sqlalchemy_engines(data, table)
        await self._run_test(engine, table)

    @given(data=data(), name=uuids())
    async def test_mapped_class(self, *, data: DataObject, name: UUID) -> None:
        class Base(DeclarativeBase, MappedAsDataclass): ...

        class Example(Base):
            __tablename__ = "example"

            id_: Mapped[int] = mapped_column(Integer, kw_only=True, primary_key=True)

        engine = await sqlalchemy_engines(data, Example)
        await self._run_test(engine, Example)

    async def _run_test(
        self,
        engine_or_conn: AsyncEngineOrConnection,
        table_or_mapped_class: TableOrMappedClass,
        /,
    ) -> None:
        for _ in range(2):
            await ensure_tables_dropped(engine_or_conn, table_or_mapped_class)
        sel = select(get_table(table_or_mapped_class))
        with raises(DatabaseError):
            async with yield_connection(engine_or_conn) as conn:
                _ = await conn.execute(sel)


class TestGetChunkSize:
    @given(data=data(), chunk_size_frac=floats(0.0, 1.0), scaling=floats(0.1, 10.0))
    async def test_main(
        self, *, data: DataObject, chunk_size_frac: float, scaling: float
    ) -> None:
        engine = await aiosqlite_engines(data)
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

    def _run_test(self, table_or_mapped_class: TableOrMappedClass, /) -> None:
        assert get_column_names(table_or_mapped_class) == ["id_"]


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

    def _run_test(self, table_or_mapped_class: TableOrMappedClass, /) -> None:
        columns = get_columns(table_or_mapped_class)
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

    def test_postgres(self) -> None:
        engine = create_async_engine("postgresql")
        assert _get_dialect(engine) == "postgresql"

    @mark.skipif(
        condition=not is_installed("asyncpg"), reason="'asyncpg' not installed"
    )
    def test_postgres_async(self) -> None:
        engine = create_async_engine("postgresql+asyncpg")
        assert _get_dialect(engine) == "postgresql"

    def test_sqlite(self) -> None:
        engine = create_async_engine("sqlite")
        assert _get_dialect(engine) == "sqlite"

    @given(data=data())
    async def test_sqlite_async(self, *, data: DataObject) -> None:
        engine = await aiosqlite_engines(data)
        assert _get_dialect(engine) == "sqlite"


class TestGetDialectMaxParams:
    @mark.parametrize("dialect", get_args(Dialect))
    def test_max_params(self, *, dialect: Dialect) -> None:
        max_params = _get_dialect_max_params(dialect)
        assert isinstance(max_params, int)


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

        table = get_table(Example)
        result = get_table(table)
        assert result is Example.__table__

    def test_error(self) -> None:
        with raises(
            GetTableError, match="Object .* must be a Table or mapped class; got .*"
        ):
            _ = get_table(cast(Any, type(None)))


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


class TestInsertItems:
    @given(data=data(), id_=integers(0, 10))
    @mark.parametrize("case", [param("tuple"), param("dict")])
    @mark.parametrize("use_conn", [param(True), param(False)])
    async def test_pair_of_obj_and_table(
        self,
        *,
        case: Literal["tuple", "dict"],
        data: DataObject,
        id_: int,
        use_conn: bool,
    ) -> None:
        engine = await aiosqlite_engines(data)
        match case:
            case "tuple":
                item = (id_,), self._table
            case "dict":
                item = {"id_": id_}, self._table
        await self._run_test(engine, {id_}, item, use_conn=use_conn)

    @given(data=data(), ids=sets(integers(0, 10), min_size=1))
    @mark.parametrize("case", [param("tuple"), param("dict")])
    @mark.parametrize("use_conn", [param(True), param(False)])
    async def test_pair_of_objs_and_table(
        self,
        *,
        case: Literal["tuple", "dict"],
        data: DataObject,
        ids: set[int],
        use_conn: bool,
    ) -> None:
        engine = await aiosqlite_engines(data)
        match case:
            case "tuple":
                item = [((id_,)) for id_ in ids], self._table
            case "dict":
                item = [({"id_": id_}) for id_ in ids], self._table
        await self._run_test(engine, ids, item, use_conn=use_conn)

    @given(data=data(), ids=sets(integers(0, 10), min_size=1))
    @mark.parametrize("case", [param("tuple"), param("dict")])
    @mark.parametrize("use_conn", [param(True), param(False)])
    async def test_list_of_pairs_of_objs_and_table(
        self,
        *,
        case: Literal["tuple", "dict"],
        data: DataObject,
        ids: set[int],
        use_conn: bool,
    ) -> None:
        engine = await aiosqlite_engines(data)
        match case:
            case "tuple":
                item = [((id_,), self._table) for id_ in ids]
            case "dict":
                item = [({"id_": id_}, self._table) for id_ in ids]
        await self._run_test(engine, ids, item, use_conn=use_conn)

    @given(data=data(), ids=sets(integers(0, 1000), min_size=10, max_size=100))
    @mark.parametrize("use_conn", [param(True), param(False)])
    async def test_many_items(
        self, *, data: DataObject, ids: set[int], use_conn: bool
    ) -> None:
        engine = await aiosqlite_engines(data)
        await self._run_test(
            engine, ids, [({"id_": id_}, self._table) for id_ in ids], use_conn=use_conn
        )

    @given(data=data(), id_=integers(0, 10))
    @mark.parametrize("use_conn", [param(True), param(False)])
    async def test_mapped_class(
        self, *, data: DataObject, id_: int, use_conn: bool
    ) -> None:
        engine = await aiosqlite_engines(data)
        await self._run_test(
            engine, {id_}, self._mapped_class(id_=id_), use_conn=use_conn
        )

    @given(data=data(), ids=sets(integers(0, 10), min_size=1))
    @mark.parametrize("use_conn", [param(True), param(False)])
    async def test_mapped_classes(
        self, *, data: DataObject, ids: set[int], use_conn: bool
    ) -> None:
        engine = await aiosqlite_engines(data)
        await self._run_test(
            engine, ids, [self._mapped_class(id_=id_) for id_ in ids], use_conn=use_conn
        )

    @given(data=data(), id_=integers(0, 10))
    @mark.parametrize("use_conn", [param(True), param(False)])
    async def test_assume_table_exists(
        self, *, data: DataObject, id_: int, use_conn: bool
    ) -> None:
        engine = await aiosqlite_engines(data)
        await self._run_test(
            engine,
            {id_},
            self._mapped_class(id_=id_),
            assume_tables_exist=True,
            use_conn=use_conn,
        )

    @given(data=data())
    @mark.parametrize("use_conn", [param(True), param(False)])
    async def test_error(self, *, data: DataObject, use_conn: bool) -> None:
        engine = await aiosqlite_engines(data)
        with raises(InsertItemsError, match="Item must be valid; got None"):
            await self._run_test(engine, set(), cast(Any, None), use_conn=use_conn)

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

    async def _run_test(
        self,
        engine_or_conn: AsyncEngineOrConnection,
        ids: set[int],
        /,
        *items: _InsertItem,
        assume_tables_exist: bool = False,
        use_conn: bool = False,
    ) -> None:
        if use_conn:
            async with yield_connection(engine_or_conn) as conn:
                await self._run_test(
                    conn, ids, *items, assume_tables_exist=assume_tables_exist
                )
            return
        if assume_tables_exist:
            with raises(OperationalError, match="no such table"):
                await insert_items(
                    engine_or_conn, *items, assume_tables_exist=assume_tables_exist
                )
            return
        await insert_items(
            engine_or_conn, *items, assume_tables_exist=assume_tables_exist
        )
        sel = self._get_select(self._table)
        async with yield_connection(engine_or_conn) as conn:
            results = (await conn.execute(sel)).scalars().all()
        self._assert_results(results, ids)

    def _get_select(self, table_or_mapped_class: TableOrMappedClass, /) -> Select[Any]:
        return select(get_table(table_or_mapped_class).c["id_"])

    def _assert_results(self, results: Sequence[Any], ids: set[int], /) -> None:
        assert set(results) == ids


class TestIsInsertItemPair:
    @mark.parametrize(
        ("obj", "expected"),
        [
            param(None, False),
            param((), False),
            param((1,), False),
            param((1, 2), False),
            param(((1, 2, 3), None), False),
            param(((1, 2, 3), Table("example", MetaData())), True),
            param(({"a": 1, "b": 2, "c": 3}, None), False),
            param(({"a": 1, "b": 2, "c": 3}, Table("example", MetaData())), True),
        ],
    )
    def test_main(self, *, obj: Any, expected: bool) -> None:
        result = _is_insert_item_pair(obj)
        assert result is expected


class TestIsUpsertItemPair:
    @mark.parametrize(
        ("obj", "expected"),
        [
            param(None, False),
            param((), False),
            param((1,), False),
            param((1, 2), False),
            param(((1, 2, 3), None), False),
            param(((1, 2, 3), Table("example", MetaData())), True),
            param(({"a": 1, "b": 2, "c": 3}, None), False),
            param(({"a": 1, "b": 2, "c": 3}, Table("example", MetaData())), True),
        ],
    )
    def test_main(self, *, obj: Any, expected: bool) -> None:
        result = _is_upsert_item_pair(obj)
        assert result is expected


class TestIsMappedClass:
    def test_mapped_class_instance(self) -> None:
        class Base(DeclarativeBase, MappedAsDataclass): ...

        class Example(Base):
            __tablename__ = "example"
            id_: Mapped[int] = mapped_column(Integer, kw_only=True, primary_key=True)

        assert is_mapped_class(Example)
        assert is_mapped_class(Example(id_=1))

    def test_other(self) -> None:
        assert not is_mapped_class(None)


class TestIsTableOrMappedClass:
    def test_table(self) -> None:
        table = Table("example", MetaData(), Column("id_", Integer, primary_key=True))
        assert is_table_or_mapped_class(table)

    def test_mapped_class(self) -> None:
        class Base(DeclarativeBase, MappedAsDataclass): ...

        class Example(Base):
            __tablename__ = "example"

            id_: Mapped[int] = mapped_column(Integer, kw_only=True, primary_key=True)

        assert is_table_or_mapped_class(Example)
        assert is_table_or_mapped_class(Example(id_=1))

    def test_other(self) -> None:
        assert not is_table_or_mapped_class(None)


class TestMappedClassToDict:
    @given(id_=integers())
    def test_main(self, *, id_: int) -> None:
        class Base(DeclarativeBase, MappedAsDataclass): ...

        class Example(Base):
            __tablename__ = "example"

            id_: Mapped[int] = mapped_column(Integer, kw_only=True, primary_key=True)

        example = Example(id_=id_)
        result = mapped_class_to_dict(example)
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
        result = mapped_class_to_dict(example)
        expected = {"id": id_}
        assert result == expected


class TestNormalizeInsertItem:
    @given(id_=integers())
    @mark.parametrize("case", [param("tuple"), param("dict")])
    def test_pair_of_obj_and_table(
        self, *, case: Literal["tuple", "dict"], id_: int
    ) -> None:
        table = self._table
        match case:
            case "tuple":
                item = (id_,), table
            case "dict":
                item = {"id": id_}, table
        result = one(_normalize_insert_item(item))
        expected = _NormalizedInsertItem(values=item[0], table=table)
        assert result == expected

    @given(ids=sets(integers()))
    @mark.parametrize("case", [param("tuple"), param("dict")])
    def test_pair_of_list_of_objs_and_table(
        self, *, case: Literal["tuple", "dict"], ids: set[int]
    ) -> None:
        table = self._table
        match case:
            case "tuple":
                item = [((id_,)) for id_ in ids], table
            case "dict":
                item = [({"id_": id_}) for id_ in ids], table
        result = list(_normalize_insert_item(item))
        expected = [_NormalizedInsertItem(values=i, table=table) for i in item[0]]
        assert result == expected

    @given(ids=sets(integers()))
    @mark.parametrize("case", [param("tuple"), param("dict")])
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
        expected = [_NormalizedInsertItem(values=i[0], table=table) for i in item]
        assert result == expected

    @given(id_=integers())
    def test_mapped_class(self, *, id_: int) -> None:
        cls = self._mapped_class
        result = one(_normalize_insert_item(cls(id_=id_)))
        expected = _NormalizedInsertItem(values={"id_": id_}, table=get_table(cls))
        assert result == expected

    @given(ids=sets(integers(0, 10), min_size=1))
    def test_mapped_classes(self, *, ids: set[int]) -> None:
        cls = self._mapped_class
        result = list(_normalize_insert_item([cls(id_=id_) for id_ in ids]))
        expected = [
            _NormalizedInsertItem(values={"id_": id_}, table=get_table(cls))
            for id_ in ids
        ]
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


class TestNormalizeUpsertItem:
    @given(id_=integers())
    def test_pair_of_dict_and_table(self, *, id_: int) -> None:
        table = self._table
        item = {"id": id_}, table
        result = one(_normalize_upsert_item(item))
        expected = _NormalizedUpsertItem(values=item[0], table=table)
        assert result == expected

    @given(ids=sets(integers()))
    def test_pair_of_list_of_dicts_and_table(self, *, ids: set[int]) -> None:
        table = self._table
        item = [({"id_": id_}) for id_ in ids], table
        result = list(_normalize_upsert_item(item))
        expected = [_NormalizedUpsertItem(values=i, table=table) for i in item[0]]
        assert result == expected

    @given(ids=sets(integers()))
    def test_list_of_pairs_of_dicts_and_table(self, *, ids: set[int]) -> None:
        table = self._table
        item = [({"id_": id_}, table) for id_ in ids]
        result = list(_normalize_upsert_item(item))
        expected = [_NormalizedUpsertItem(values=i[0], table=table) for i in item]
        assert result == expected

    @given(id_=integers())
    def test_mapped_class(self, *, id_: int) -> None:
        cls = self._mapped_class
        result = one(_normalize_upsert_item(cls(id_=id_)))
        expected = _NormalizedUpsertItem(values={"id_": id_}, table=get_table(cls))
        assert result == expected

    @given(ids=sets(integers(0, 10), min_size=1))
    def test_mapped_classes(self, *, ids: set[int]) -> None:
        cls = self._mapped_class
        result = list(_normalize_upsert_item([cls(id_=id_) for id_ in ids]))
        expected = [
            _NormalizedUpsertItem(values={"id_": id_}, table=get_table(cls))
            for id_ in ids
        ]
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
        with raises(_NormalizeUpsertItemError, match="Item must be valid; got .*"):
            _ = list(_normalize_upsert_item(item))

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


class TestPrepareInsertOrUpsertItems:
    @given(
        data=data(),
        normalize_item=sampled_from([_normalize_insert_item, _normalize_upsert_item]),
    )
    async def test_error(
        self, *, data: DataObject, normalize_item: Callable[[Any], Iterator[Any]]
    ) -> None:
        engine = await sqlalchemy_engines(data)
        with raises(
            _PrepareInsertOrUpsertItemsError, match="Item must be valid; got None"
        ):
            _ = _prepare_insert_or_upsert_items(
                normalize_item, engine, cast(Any, None), cast(Any, None)
            )


class TestSelectableToString:
    @given(data=data())
    @settings(max_examples=1)
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


class TestSerializeEngine:
    @given(data=data())
    def test_main(self, *, data: DataObject) -> None:
        engine = data.draw(sqlite_engines())
        result = parse_engine(serialize_engine(engine))
        assert result.url == engine.url


class TestTablenameMixin:
    def test_main(self) -> None:
        class Base(DeclarativeBase, MappedAsDataclass, TablenameMixin): ...

        class Example(Base):
            id_: Mapped[int] = mapped_column(Integer, kw_only=True, primary_key=True)

        assert get_table_name(Example) == "example"


class TestUpsertItems:
    @given(data=data(), triple=_upsert_triples(nullable=True))
    @mark.parametrize("dialect", [param("sqlite"), param("postgres", marks=SKIPIF_CI)])
    async def test_async_pair_of_dict_and_table(
        self,
        *,
        data: DataObject,
        create_postgres_engine_async: Callable[..., Coroutine1[AsyncEngine]],
        dialect: Literal["sqlite", "postgres"],
        triple: tuple[int, bool, bool | None],
    ) -> None:
        key = TestUpsertItems.test_async_pair_of_dict_and_table.__qualname__, dialect
        name = f"test_{md5_hash(key)}"
        table = self._get_table(name)
        engine = await self._get_engine_async(
            data, create_postgres_engine_async, table, dialect=dialect
        )
        id_, init, post = triple
        _ = await self._run_test_async(
            engine, table, ({"id_": id_, "value": init}, table), expected={(id_, init)}
        )
        _ = await self._run_test_async(
            engine,
            table,
            ({"id_": id_, "value": post}, table),
            expected={(id_, init if post is None else post)},
        )

    @given(data=data(), triples=_upsert_lists(nullable=True, min_size=1))
    @mark.parametrize("dialect", [param("sqlite"), param("postgres", marks=SKIPIF_CI)])
    async def test_async_pair_of_list_of_dicts_and_table(
        self,
        *,
        data: DataObject,
        create_postgres_engine_async: Callable[..., Coroutine1[AsyncEngine]],
        dialect: Literal["sqlite", "postgres"],
        triples: list[tuple[int, bool, bool | None]],
    ) -> None:
        key = (
            TestUpsertItems.test_async_pair_of_list_of_dicts_and_table.__qualname__,
            dialect,
        )
        name = f"test_{md5_hash(key)}"
        table = self._get_table(name)
        engine = await self._get_engine_async(
            data, create_postgres_engine_async, table, dialect=dialect
        )
        _ = await self._run_test_async(
            engine,
            table,
            ([{"id_": id_, "value": init} for id_, init, _ in triples], table),
            expected={(id_, init) for id_, init, _ in triples},
        )
        items = (
            [
                {"id_": id_, "value": post}
                for id_, _, post in triples
                if post is not None
            ],
            table,
        )
        expected = {
            (id_, init if post is None else post) for id_, init, post in triples
        }
        _ = await self._run_test_async(engine, table, items, expected=expected)

    @given(data=data(), triples=_upsert_lists(nullable=True, min_size=1))
    @mark.parametrize("dialect", [param("sqlite"), param("postgres", marks=SKIPIF_CI)])
    async def test_async_list_of_pairs_of_dicts_and_table(
        self,
        *,
        data: DataObject,
        create_postgres_engine_async: Callable[..., Coroutine1[AsyncEngine]],
        dialect: Literal["sqlite", "postgres"],
        triples: list[tuple[int, bool, bool | None]],
    ) -> None:
        key = (
            TestUpsertItems.test_async_list_of_pairs_of_dicts_and_table.__qualname__,
            dialect,
        )
        name = f"test_{md5_hash(key)}"
        table = self._get_table(name)
        engine = await self._get_engine_async(
            data, create_postgres_engine_async, table, dialect=dialect
        )
        _ = await self._run_test_async(
            engine,
            table,
            ([{"id_": id_, "value": init} for id_, init, _ in triples], table),
            expected={(id_, init) for id_, init, _ in triples},
        )
        items = [
            ({"id_": id_, "value": post}, table)
            for id_, _, post in triples
            if post is not None
        ]
        expected = {
            (id_, init if post is None else post) for id_, init, post in triples
        }
        _ = await self._run_test_async(engine, table, items, expected=expected)

    @given(data=data(), triple=_upsert_triples())
    @mark.parametrize("dialect", [param("sqlite"), param("postgres", marks=SKIPIF_CI)])
    async def test_async_mapped_class(
        self,
        *,
        data: DataObject,
        create_postgres_engine_async: Callable[..., Coroutine1[AsyncEngine]],
        dialect: Literal["sqlite", "postgres"],
        triple: tuple[int, bool, bool],
    ) -> None:
        key = TestUpsertItems.test_async_mapped_class.__qualname__, dialect
        name = f"test_{md5_hash(key)}"
        cls = self._get_mapped_class(name)
        engine = await self._get_engine_async(
            data, create_postgres_engine_async, cls, dialect=dialect
        )
        id_, init, post = triple
        _ = await self._run_test_async(
            engine, cls, cls(id_=id_, value=init), expected={(id_, init)}
        )
        _ = await self._run_test_async(
            engine, cls, cls(id_=id_, value=post), expected={(id_, post)}
        )

    @given(data=data(), triples=_upsert_lists(nullable=True, min_size=1))
    @mark.parametrize("dialect", [param("sqlite"), param("postgres", marks=SKIPIF_CI)])
    async def test_async_mapped_classes(
        self,
        *,
        data: DataObject,
        create_postgres_engine_async: Callable[..., Coroutine1[AsyncEngine]],
        dialect: Literal["sqlite", "postgres"],
        triples: list[tuple[int, bool, bool | None]],
    ) -> None:
        key = TestUpsertItems.test_async_mapped_classes.__qualname__, dialect
        name = f"test_{md5_hash(key)}"
        cls = self._get_mapped_class(name)
        engine = await self._get_engine_async(
            data, create_postgres_engine_async, cls, dialect=dialect
        )
        _ = await self._run_test_async(
            engine,
            cls,
            [cls(id_=id_, value=init) for id_, init, _ in triples],
            expected={(id_, init) for id_, init, _ in triples},
        )
        items = [
            cls(id_=id_, value=post) for id_, _, post in triples if post is not None
        ]
        expected = {
            (id_, init if post is None else post) for id_, init, post in triples
        }
        _ = await self._run_test_async(engine, cls, items, expected=expected)

    @given(
        data=data(),
        id_=integers(0, 10),
        x_init=booleans(),
        x_post=booleans(),
        y=booleans(),
    )
    @mark.parametrize("dialect", [param("sqlite"), param("postgres", marks=SKIPIF_CI)])
    @mark.parametrize("selected_or_all", [param("selected"), param("all")])
    async def test_async_sel_or_all(
        self,
        *,
        data: DataObject,
        create_postgres_engine_async: Callable[..., Coroutine1[AsyncEngine]],
        dialect: Literal["sqlite", "postgres"],
        selected_or_all: Literal["selected", "all"],
        id_: int,
        x_init: bool,
        x_post: bool,
        y: bool,
    ) -> None:
        key = (
            TestUpsertItems.test_async_sel_or_all.__qualname__,
            dialect,
            selected_or_all,
        )
        name = f"test_{md5_hash(key)}"
        table = self._get_table_sel_or_all(name)
        engine = await self._get_engine_async(
            data, create_postgres_engine_async, table, dialect=dialect
        )
        _ = await self._run_test_async(
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
        _ = await self._run_test_async(
            engine,
            table,
            ({"id_": id_, "x": x_post}, table),
            selected_or_all=selected_or_all,
            expected={expected},
        )

    @given(data=data(), triple=_upsert_triples())
    @mark.parametrize("dialect", [param("sqlite"), param("postgres", marks=SKIPIF_CI)])
    @mark.parametrize("single_or_list", [param("single"), param("list")])
    async def test_async_updated(
        self,
        *,
        data: DataObject,
        create_postgres_engine_async: Callable[..., Coroutine1[AsyncEngine]],
        dialect: Literal["sqlite", "postgres"],
        single_or_list: Literal["single", "list"],
        triple: tuple[int, bool, bool | None],
    ) -> None:
        key = TestUpsertItems.test_async_updated.__qualname__, dialect, single_or_list
        name = f"test_{md5_hash(key)}"
        table = self._get_table_updated(name)
        engine = await self._get_engine_async(
            data, create_postgres_engine_async, table, dialect=dialect
        )
        id_, init, post = triple
        match single_or_list:
            case "single":
                item1 = ({"id_": id_, "value": init}, table)
            case "list":
                item1 = [({"id_": id_, "value": init}, table)]
        ((_, _, updated1),) = await self._run_test_async(engine, table, item1)
        await asyncio.sleep(0.01)
        match single_or_list:
            case "single":
                item2 = ({"id_": id_, "value": post}, table)
            case "list":
                item2 = [({"id_": id_, "value": post}, table)]
        ((_, _, updated2),) = await self._run_test_async(engine, table, item2)
        assert updated1 < updated2

    @given(data=data(), id_=integers(0, 10))
    @mark.parametrize("dialect", [param("sqlite"), param("postgres", marks=SKIPIF_CI)])
    async def test_async_assume_table_exists(
        self,
        *,
        data: DataObject,
        create_postgres_engine_async: Callable[..., Coroutine1[AsyncEngine]],
        dialect: Literal["sqlite", "postgres"],
        id_: int,
    ) -> None:
        key = TestUpsertItems.test_async_assume_table_exists.__qualname__, dialect
        name = f"test_{md5_hash(key)}"
        table = self._get_table(name)
        engine = await self._get_engine_async(
            data, create_postgres_engine_async, table, dialect=dialect
        )
        _ = await self._run_test_async(
            engine, table, ({"id_": id_}, table), assume_tables_exist=True
        )

    @given(
        data=data(),
        ids=sets_fixed_length(int32s(), 2).map(tuple),
        value1=booleans() | none(),
        value2=booleans() | none(),
    )
    @mark.parametrize("dialect", [param("sqlite"), param("postgres", marks=SKIPIF_CI)])
    async def test_async_both_nulls_and_non_nulls(
        self,
        *,
        data: DataObject,
        create_postgres_engine_async: Callable[..., Coroutine1[AsyncEngine]],
        dialect: Literal["sqlite", "postgres"],
        ids: tuple[int, int],
        value1: bool | None,
        value2: bool | None,
    ) -> None:
        key = TestUpsertItems.test_async_both_nulls_and_non_nulls.__qualname__, dialect
        name = f"test_{md5_hash(key)}"
        table = self._get_table(name)
        engine = await self._get_engine_async(
            data, create_postgres_engine_async, table, dialect=dialect
        )
        id1, id2 = ids
        await upsert_items(
            engine,
            ([{"id_": id1, "value": value1}, {"id_": id2, "value": value2}], table),
        )

    @given(data=data())
    @mark.parametrize("dialect", [param("sqlite"), param("postgres", marks=SKIPIF_CI)])
    async def test_async_error(
        self,
        *,
        data: DataObject,
        create_postgres_engine_async: Callable[..., Coroutine1[AsyncEngine]],
        dialect: Literal["sqlite", "postgres"],
    ) -> None:
        key = TestUpsertItems.test_async_error.__qualname__, dialect
        name = f"test_{md5_hash(key)}"
        table = self._get_table(name)
        engine = await self._get_engine_async(
            data, create_postgres_engine_async, table, dialect=dialect
        )
        with raises(UpsertItemsAsyncError, match="Item must be valid; got None"):
            _ = await self._run_test_async(engine, table, cast(Any, None))

    def _get_table(self, name: str, /) -> Table:
        return Table(
            name,
            MetaData(),
            Column("id_", Integer, primary_key=True),
            Column("value", Boolean, nullable=True),
        )

    def _get_table_sel_or_all(self, name: str, /) -> Table:
        return Table(
            name,
            MetaData(),
            Column("id_", Integer, primary_key=True),
            Column("x", Boolean, nullable=False),
            Column("y", Boolean, nullable=True),
        )

    def _get_table_updated(self, name: str, /) -> Table:
        return Table(
            name,
            MetaData(),
            Column("id_", Integer, primary_key=True),
            Column("value", Boolean, nullable=False),
            Column(
                "updated_at",
                DateTime(timezone=True),
                server_default=func.now(),
                onupdate=func.now(),
            ),
        )

    def _get_mapped_class(self, name: str, /) -> type[DeclarativeBase]:
        class Base(DeclarativeBase, MappedAsDataclass): ...

        class Example(Base):
            __tablename__ = name

            id_: Mapped[int] = mapped_column(Integer, kw_only=True, primary_key=True)
            value: Mapped[bool] = mapped_column(Boolean, kw_only=True, nullable=False)

        return Example

    def _get_engine_sync(
        self,
        sqlite_engine: Engine,
        create_postgres_engine: Callable[..., Engine],
        table_or_mapped_class: TableOrMappedClass,
        /,
        *,
        dialect: Literal["sqlite", "postgres"],
    ) -> Engine:
        match dialect:
            case "sqlite":
                return sqlite_engine
            case "postgres":
                return create_postgres_engine(table_or_mapped_class)

    async def _get_engine_async(
        self,
        data: DataObject,
        create_postgres_engine_async: Callable[..., Coroutine1[AsyncEngine]],
        table_or_mapped_class: TableOrMappedClass,
        /,
        *,
        dialect: Literal["sqlite", "postgres"],
    ) -> AsyncEngine:
        match dialect:
            case "sqlite":
                return await aiosqlite_engines(data)
            case "postgres":
                return await create_postgres_engine_async(table_or_mapped_class)

    def _run_test_sync(
        self,
        engine_or_conn: EngineOrConnection,
        table_or_mapped_class: TableOrMappedClass,
        /,
        *items: _UpsertItem,
        assume_tables_exist: bool = False,
        selected_or_all: Literal["selected", "all"] = "selected",
        expected: set[tuple[Any, ...]] | None = None,
    ) -> Sequence[Row[Any]]:
        if assume_tables_exist:
            with raises((OperationalError, ProgrammingError)):
                upsert_items(
                    engine_or_conn,
                    *items,
                    assume_tables_exist=assume_tables_exist,
                    selected_or_all=selected_or_all,
                )
            return []
        upsert_items(
            engine_or_conn,
            *items,
            assume_tables_exist=assume_tables_exist,
            selected_or_all=selected_or_all,
        )
        sel = self._get_select(table_or_mapped_class)
        with yield_connection(engine_or_conn) as conn:
            results = conn.execute(sel).all()
        if expected is not None:
            self._assert_results(results, expected)
        return results

    async def _run_test_async(
        self,
        engine_or_conn: AsyncEngineOrConnection,
        table_or_mapped_class: TableOrMappedClass,
        /,
        *items: _UpsertItem,
        assume_tables_exist: bool = False,
        selected_or_all: Literal["selected", "all"] = "selected",
        expected: set[tuple[Any, ...]] | None = None,
    ) -> Sequence[Row[Any]]:
        if assume_tables_exist:
            with raises((OperationalError, ProgrammingError)):
                await upsert_items(
                    engine_or_conn,
                    *items,
                    selected_or_all=selected_or_all,
                    assume_tables_exist=assume_tables_exist,
                )
            return []
        await upsert_items(
            engine_or_conn,
            *items,
            selected_or_all=selected_or_all,
            assume_tables_exist=assume_tables_exist,
        )
        sel = self._get_select(table_or_mapped_class)
        async with yield_connection(engine_or_conn) as conn:
            results = (await conn.execute(sel)).all()
        if expected is not None:
            self._assert_results(results, expected)
        return results

    def _get_select(self, table_or_mapped_class: TableOrMappedClass, /) -> Select[Any]:
        return select(get_table(table_or_mapped_class))

    def _assert_results(self, results: Sequence[Any], ids: set[Any], /) -> None:
        assert set(results) == ids


class TestYieldConnection:
    @given(data=data())
    async def test_engine(self, *, data: DataObject) -> None:
        engine = await aiosqlite_engines(data)
        async with yield_connection(engine) as conn:
            assert isinstance(conn, AsyncConnection)

    @given(data=data())
    async def test_conn(self, *, data: DataObject) -> None:
        engine = await aiosqlite_engines(data)
        async with engine.begin() as conn1, yield_connection(conn1) as conn2:
            assert isinstance(conn2, AsyncConnection)


class TestYieldPrimaryKeyColumns:
    def test_main(self) -> None:
        table = Table(
            "example",
            MetaData(),
            Column("id1", Integer, primary_key=True),
            Column("id2", Integer, primary_key=True),
            Column("id3", Integer),
        )
        columns = list(yield_primary_key_columns(table))
        expected = [
            Column("id1", Integer, primary_key=True),
            Column("id2", Integer, primary_key=True),
        ]
        for c, e in zip(columns, expected, strict=True):
            assert c.name == e.name
            assert c.primary_key == e.primary_key
