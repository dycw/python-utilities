from __future__ import annotations

from time import time_ns
from typing import TYPE_CHECKING, Any, Literal, cast, overload

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
    uuids,
)
from pytest import mark, param, raises
from sqlalchemy import Boolean, Column, Integer, MetaData, Table, select
from sqlalchemy.exc import DatabaseError, OperationalError, ProgrammingError
from sqlalchemy.ext.asyncio import AsyncEngine
from sqlalchemy.orm import DeclarativeBase, Mapped, MappedAsDataclass, mapped_column

from tests.conftest import FLAKY
from utilities.hypothesis import int32s, sqlalchemy_engines, temp_paths
from utilities.iterables import one
from utilities.modules import is_installed
from utilities.sqlalchemy import (
    CheckEngineError,
    Dialect,
    GetTableError,
    InsertItemsError,
    TablenameMixin,
    TableOrMappedClass,
    UpsertItemsError,
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
    check_engine,
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
    upsert_items,
    yield_primary_key_columns,
)
from utilities.text import strip_and_dedent
from utilities.typing import get_args

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator
    from pathlib import Path


def _table_names() -> SearchStrategy[str]:
    now = time_ns()
    return uuids().map(lambda x: f"test_{now}_{str(x).replace('-', '')}")


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


class TestCheckEngine:
    @FLAKY
    @given(data=data())
    @settings(phases={Phase.generate})
    async def test_main(self, *, data: DataObject) -> None:
        engine = await sqlalchemy_engines(data)
        await check_engine(engine)

    @FLAKY
    @given(data=data(), name=_table_names())
    @settings(phases={Phase.generate})
    async def test_num_tables_pass(self, *, data: DataObject, name: str) -> None:
        table = Table(name, MetaData(), Column("id", Integer, primary_key=True))
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

    @FLAKY
    @given(data=data())
    @settings(phases={Phase.generate})
    async def test_num_tables_error(self, *, data: DataObject) -> None:
        engine = await sqlalchemy_engines(data)
        with raises(CheckEngineError, match=r".* must have 100000 table\(s\); got .*"):
            await check_engine(engine, num_tables=100000)


class TestColumnwiseMinMax:
    @FLAKY
    @given(
        data=data(),
        name=_table_names(),
        values=sets(
            tuples(integers(0, 10) | none(), integers(0, 10) | none()), min_size=1
        ),
    )
    @settings(phases={Phase.generate})
    async def test_main(
        self, *, data: DataObject, name: str, values: set[tuple[int | None, int | None]]
    ) -> None:
        table = Table(
            name,
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

    @FLAKY
    @given(data=data(), name=_table_names())
    @settings(phases={Phase.generate})
    async def test_label(self, *, data: DataObject, name: str) -> None:
        table = Table(
            name,
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
    @FLAKY
    @given(temp_path=temp_paths())
    @settings(phases={Phase.generate})
    def test_async(self, *, temp_path: Path) -> None:
        engine = create_async_engine("sqlite+aiosqlite", database=temp_path.name)
        assert isinstance(engine, AsyncEngine)

    @FLAKY
    @given(temp_path=temp_paths())
    @settings(phases={Phase.generate})
    def test_query(self, *, temp_path: Path) -> None:
        engine = create_async_engine(
            "sqlite+aiosqlite",
            database=temp_path.name,
            query={"arg1": "value1", "arg2": ["value2"]},
        )
        assert isinstance(engine, AsyncEngine)


class TestEnsureTablesCreated:
    @FLAKY
    @given(data=data(), name=_table_names())
    @settings(phases={Phase.generate})
    async def test_table(self, *, data: DataObject, name: str) -> None:
        table = Table(name, MetaData(), Column("id_", Integer, primary_key=True))
        engine = await sqlalchemy_engines(data, table)
        await self._run_test(engine, table)

    @FLAKY
    @given(data=data(), name=_table_names())
    @settings(phases={Phase.generate})
    async def test_mapped_class(self, *, data: DataObject, name: str) -> None:
        class Base(DeclarativeBase, MappedAsDataclass): ...

        class Example(Base):
            __tablename__ = name

            id_: Mapped[int] = mapped_column(Integer, kw_only=True, primary_key=True)

        engine = await sqlalchemy_engines(data, Example)
        await self._run_test(engine, Example)

    async def _run_test(
        self, engine: AsyncEngine, table_or_mapped_class: TableOrMappedClass, /
    ) -> None:
        for _ in range(2):
            await ensure_tables_created(engine, table_or_mapped_class)
        sel = select(get_table(table_or_mapped_class))
        async with engine.begin() as conn:
            _ = (await conn.execute(sel)).all()


class TestEnsureTablesDropped:
    @FLAKY
    @given(data=data(), name=_table_names())
    @settings(phases={Phase.generate})
    async def test_table(self, *, data: DataObject, name: str) -> None:
        table = Table(name, MetaData(), Column("id_", Integer, primary_key=True))
        engine = await sqlalchemy_engines(data, table)
        await self._run_test(engine, table)

    @FLAKY
    @given(data=data(), name=_table_names())
    @settings(phases={Phase.generate})
    async def test_mapped_class(self, *, data: DataObject, name: str) -> None:
        class Base(DeclarativeBase, MappedAsDataclass): ...

        class Example(Base):
            __tablename__ = name

            id_: Mapped[int] = mapped_column(Integer, kw_only=True, primary_key=True)

        engine = await sqlalchemy_engines(data, Example)
        await self._run_test(engine, Example)

    async def _run_test(
        self, engine: AsyncEngine, table_or_mapped_class: TableOrMappedClass, /
    ) -> None:
        for _ in range(2):
            await ensure_tables_dropped(engine, table_or_mapped_class)
        sel = select(get_table(table_or_mapped_class))
        with raises(DatabaseError):
            async with engine.begin() as conn:
                _ = await conn.execute(sel)


class TestGetChunkSize:
    @FLAKY
    @given(data=data(), chunk_size_frac=floats(0.0, 1.0), scaling=floats(0.1, 10.0))
    @settings(phases={Phase.generate})
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
    @FLAKY
    @given(
        data=data(),
        name=_table_names(),
        case=sampled_from(["tuple", "dict"]),
        id_=integers(0, 10),
    )
    @settings(phases={Phase.generate})
    async def test_pair_of_obj_and_table(
        self, *, data: DataObject, name: str, case: Literal["tuple", "dict"], id_: int
    ) -> None:
        table = self._make_table(name)
        engine = await sqlalchemy_engines(data, table)
        match case:
            case "tuple":
                item = (id_,), table
            case "dict":
                item = {"id_": id_}, table
        await self._run_test(engine, table, {id_}, item)

    @FLAKY
    @given(
        data=data(),
        name=_table_names(),
        case=sampled_from([
            "pair-list-of-tuples",
            "pair-list-of-dicts",
            "list-of-pair-of-tuples",
            "list-of-pair-of-dicts",
        ]),
        ids=sets(integers(0, 10), min_size=1),
    )
    @settings(phases={Phase.generate})
    async def test_pair_of_objs_and_table_or_list_of_pairs_of_objs_and_table(
        self,
        *,
        data: DataObject,
        name: str,
        case: Literal[
            "pair-list-of-tuples",
            "pair-list-of-dicts",
            "list-of-pair-of-tuples",
            "list-of-pair-of-dicts",
        ],
        ids: set[int],
    ) -> None:
        table = self._make_table(name)
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

    @FLAKY
    @given(
        data=data(),
        name=_table_names(),
        ids=sets(integers(0, 1000), min_size=10, max_size=100),
    )
    @settings(phases={Phase.generate})
    async def test_many_items(
        self, *, data: DataObject, name: str, ids: set[int]
    ) -> None:
        table = self._make_table(name)
        engine = await sqlalchemy_engines(data, table)
        await self._run_test(engine, table, ids, [({"id_": id_}, table) for id_ in ids])

    @FLAKY
    @given(data=data(), name=_table_names(), id_=integers(0, 10))
    @settings(phases={Phase.generate})
    async def test_mapped_class(self, *, data: DataObject, name: str, id_: int) -> None:
        cls = self._make_mapped_class(name)
        engine = await sqlalchemy_engines(data, cls)
        await self._run_test(engine, cls, {id_}, cls(id_=id_))

    @FLAKY
    @given(data=data(), name=_table_names(), ids=sets(integers(0, 10), min_size=1))
    @settings(phases={Phase.generate})
    async def test_mapped_classes(
        self, *, data: DataObject, name: str, ids: set[int]
    ) -> None:
        cls = self._make_mapped_class(name)
        engine = await sqlalchemy_engines(data, cls)
        await self._run_test(engine, cls, ids, [cls(id_=id_) for id_ in ids])

    @FLAKY
    @given(data=data(), name=_table_names(), id_=integers(0, 10))
    @settings(phases={Phase.generate})
    async def test_assume_table_exists(
        self, *, data: DataObject, name: str, id_: int
    ) -> None:
        table = self._make_table(name)
        engine = await sqlalchemy_engines(data, table)
        with raises(
            (OperationalError, ProgrammingError), match="(no such table|does not exist)"
        ):
            await insert_items(engine, ({"id_": id_}, table), assume_tables_exist=True)

    @FLAKY
    @given(data=data(), name=_table_names())
    @settings(phases={Phase.generate})
    async def test_error(self, *, data: DataObject, name: str) -> None:
        cls = self._make_mapped_class(name)
        engine = await sqlalchemy_engines(data, cls)
        with raises(InsertItemsError, match="Item must be valid; got None"):
            await self._run_test(engine, cls, set(), cast(Any, None))

    def _make_table(self, name: str, /) -> Table:
        return Table(name, MetaData(), Column("id_", Integer, primary_key=True))

    def _make_mapped_class(self, name: str, /) -> type[DeclarativeBase]:
        class Base(DeclarativeBase, MappedAsDataclass): ...

        class Example(Base):
            __tablename__ = name

            id_: Mapped[int] = mapped_column(Integer, kw_only=True, primary_key=True)

        return Example

    async def _run_test(
        self,
        engine: AsyncEngine,
        table_or_mapped_class: TableOrMappedClass,
        ids: set[int],
        /,
        *items: _InsertItem,
    ) -> None:
        await insert_items(engine, *items)
        sel = select(get_table(table_or_mapped_class).c["id_"])
        async with engine.begin() as conn:
            results = (await conn.execute(sel)).scalars().all()
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
            param(((1, 2, 3), Table("example", MetaData())), False),
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
    @given(case=sampled_from(["tuple", "dict"]), id_=integers(0, 10))
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

    @given(case=sampled_from(["tuple", "dict"]), ids=sets(integers(0, 10)))
    def test_pair_of_objs_and_table(
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
    @FLAKY
    @given(
        data=data(),
        normalize_item=sampled_from([_normalize_insert_item, _normalize_upsert_item]),
    )
    @settings(phases={Phase.generate})
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
    @FLAKY
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


class TestTablenameMixin:
    def test_main(self) -> None:
        class Base(DeclarativeBase, MappedAsDataclass, TablenameMixin): ...

        class Example(Base):
            id_: Mapped[int] = mapped_column(Integer, kw_only=True, primary_key=True)

        assert get_table_name(Example) == "example"


class TestUpsertItems:
    @FLAKY
    @given(data=data(), name=_table_names(), triple=_upsert_triples(nullable=True))
    @settings(phases={Phase.generate})
    async def test_pair_of_dict_and_table(
        self, *, data: DataObject, name: str, triple: tuple[int, bool, bool | None]
    ) -> None:
        table = self._make_table(name)
        engine = await sqlalchemy_engines(data, table)
        id_, init, post = triple
        init_item = {"id_": id_, "value": init}, table
        await self._run_test(engine, table, init_item, expected={(id_, init)})
        post_item = {"id_": id_, "value": post}, table
        _ = await self._run_test(
            engine, table, post_item, expected={(id_, init if post is None else post)}
        )

    @FLAKY
    @given(
        data=data(),
        name=_table_names(),
        triples=_upsert_lists(nullable=True, min_size=1),
        case=sampled_from(["pair-list-of-dicts", "list-of-pair-of-dicts"]),
    )
    @settings(phases={Phase.generate})
    async def test_pair_of_list_of_dicts_and_table(
        self,
        *,
        data: DataObject,
        name: str,
        triples: list[tuple[int, bool, bool | None]],
        case: Literal["pair-list-of-dicts", "list-of-pair-of-dicts"],
    ) -> None:
        table = self._make_table(name)
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

    @FLAKY
    @given(data=data(), name=_table_names(), triple=_upsert_triples())
    @settings(phases={Phase.generate})
    async def test_mapped_class(
        self, *, data: DataObject, name: str, triple: tuple[int, bool, bool]
    ) -> None:
        cls = self._make_mapped_class(name)
        engine = await sqlalchemy_engines(data, cls)
        id_, init, post = triple
        _ = await self._run_test(
            engine, cls, cls(id_=id_, value=init), expected={(id_, init)}
        )
        _ = await self._run_test(
            engine, cls, cls(id_=id_, value=post), expected={(id_, post)}
        )

    @FLAKY
    @given(
        data=data(),
        name=_table_names(),
        triples=_upsert_lists(nullable=True, min_size=1),
    )
    @settings(phases={Phase.generate})
    async def test_mapped_classes(
        self,
        *,
        data: DataObject,
        name: str,
        triples: list[tuple[int, bool, bool | None]],
    ) -> None:
        cls = self._make_mapped_class(name)
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

    @FLAKY
    @given(
        data=data(),
        name=_table_names(),
        id_=integers(0, 10),
        x_init=booleans(),
        x_post=booleans(),
        y=booleans(),
        selected_or_all=sampled_from(["selected", "all"]),
    )
    @settings(phases={Phase.generate})
    async def test_async_sel_or_all(
        self,
        *,
        data: DataObject,
        name: str,
        selected_or_all: Literal["selected", "all"],
        id_: int,
        x_init: bool,
        x_post: bool,
        y: bool,
    ) -> None:
        table = Table(
            name,
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

    @FLAKY
    @given(data=data(), name=_table_names(), id_=integers(0, 10))
    @settings(phases={Phase.generate})
    async def test_assume_table_exists(
        self, *, data: DataObject, name: str, id_: int
    ) -> None:
        table = self._make_table(name)
        engine = await sqlalchemy_engines(data, table)
        with raises((OperationalError, ProgrammingError)):
            await upsert_items(
                engine, ({"id_": id_, "value": True}, table), assume_tables_exist=True
            )

    @FLAKY
    @given(
        data=data(),
        name=_table_names(),
        id1=int32s(),
        id2=int32s(),
        value1=booleans() | none(),
        value2=booleans() | none(),
    )
    @settings(phases={Phase.generate})
    async def test_both_nulls_and_non_nulls(
        self,
        *,
        data: DataObject,
        name: str,
        id1: int,
        id2: int,
        value1: bool | None,
        value2: bool | None,
    ) -> None:
        table = self._make_table(name)
        engine = await sqlalchemy_engines(data, table)
        _ = assume(id1 != id2)
        await upsert_items(
            engine,
            ([{"id_": id1, "value": value1}, {"id_": id2, "value": value2}], table),
        )

    @FLAKY
    @given(data=data(), name=_table_names())
    @settings(phases={Phase.generate})
    async def test_error(self, *, data: DataObject, name: str) -> None:
        table = self._make_table(name)
        engine = await sqlalchemy_engines(data, table)
        with raises(UpsertItemsError, match="Item must be valid; got None"):
            _ = await self._run_test(engine, table, cast(Any, None))

    def _make_table(self, name: str, /) -> Table:
        return Table(
            name,
            MetaData(),
            Column("id_", Integer, primary_key=True),
            Column("value", Boolean, nullable=True),
        )

    def _make_mapped_class(self, name: str, /) -> type[DeclarativeBase]:
        class Base(DeclarativeBase, MappedAsDataclass): ...

        class Example(Base):
            __tablename__ = name

            id_: Mapped[int] = mapped_column(Integer, kw_only=True, primary_key=True)
            value: Mapped[bool] = mapped_column(Boolean, kw_only=True, nullable=False)

        return Example

    async def _run_test(
        self,
        engine: AsyncEngine,
        table_or_mapped_class: TableOrMappedClass,
        /,
        *items: _UpsertItem,
        selected_or_all: Literal["selected", "all"] = "selected",
        expected: set[tuple[Any, ...]] | None = None,
    ) -> None:
        await upsert_items(engine, *items, selected_or_all=selected_or_all)
        sel = select(get_table(table_or_mapped_class))
        async with engine.begin() as conn:
            results = (await conn.execute(sel)).all()
        if expected is not None:
            assert set(results) == expected


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
