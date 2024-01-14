from __future__ import annotations

from typing import Any

from hypothesis import given
from hypothesis.strategies import floats, integers, sets
from pytest import mark, param, raises
from sqlalchemy import Column, Engine, Integer, MetaData, Table, select
from sqlalchemy import create_engine as _create_engine
from sqlalchemy.orm import declarative_base

from utilities._sqlalchemy.common import (
    Dialect,
    GetTableError,
    _insert_items_collect,
    _insert_items_collect_iterable,
    _insert_items_collect_valid,
    _InsertionItem,
    _InsertItemsCollectError,
    _InsertItemsCollectIterableError,
    get_chunk_size,
    get_columns,
    get_dialect,
    get_table,
    insert_items,
    is_mapped_class,
    is_table_or_mapped_class,
    mapped_class_to_dict,
)
from utilities.hypothesis import sqlite_engines
from utilities.more_itertools import one
from utilities.pytest import skipif_not_linux
from utilities.sqlalchemy import ensure_tables_created


class TestDialect:
    @mark.parametrize("dialect", Dialect)
    def test_max_params(self, *, dialect: Dialect) -> None:
        assert isinstance(dialect.max_params, int)


class TestEnsureTablesCreated:
    @given(engine=sqlite_engines())
    @mark.parametrize("runs", [param(1), param(2)])
    def test_table(self, *, engine: Engine, runs: int) -> None:
        table = Table("example", MetaData(), Column("id_", Integer, primary_key=True))
        self._run_test(table, engine, runs)

    @given(engine=sqlite_engines())
    @mark.parametrize("runs", [param(1), param(2)])
    def test_mapped_class(self, *, engine: Engine, runs: int) -> None:
        class Example(declarative_base()):
            __tablename__ = "example"

            id_ = Column(Integer, primary_key=True)

        self._run_test(Example, engine, runs)

    def _run_test(
        self, table_or_mapped_class: Table | type[Any], engine: Engine, runs: int, /
    ) -> None:
        for _ in range(runs):
            ensure_tables_created(engine, table_or_mapped_class)
        sel = get_table(table_or_mapped_class).select()
        with engine.begin() as conn:
            _ = conn.execute(sel).all()


class TestGetChunkSize:
    @given(
        engine=sqlite_engines(),
        chunk_size_frac=floats(0.0, 1.0),
        scaling=floats(0.1, 10.0),
    )
    def test_main(
        self, *, engine: Engine, chunk_size_frac: float, scaling: float
    ) -> None:
        result = get_chunk_size(
            engine, chunk_size_frac=chunk_size_frac, scaling=scaling
        )
        assert result >= 1


class TestGetColumns:
    def test_table(self) -> None:
        table = Table("example", MetaData(), Column("id", Integer, primary_key=True))
        self._run_test(table)

    def test_mapped_class(self) -> None:
        class Example(declarative_base()):
            __tablename__ = "example"

            id_ = Column(Integer, primary_key=True)

        self._run_test(Example)

    def _run_test(self, table_or_mapped_class: Table | type[Any], /) -> None:
        columns = get_columns(table_or_mapped_class)
        assert isinstance(columns, list)
        assert len(columns) == 1
        assert isinstance(columns[0], Column)


class TestGetDialect:
    @given(engine=sqlite_engines())
    def test_sqlite(self, *, engine: Engine) -> None:
        assert get_dialect(engine) is Dialect.sqlite

    @mark.parametrize(
        ("url", "expected"),
        [
            param(
                "mssql+pyodbc://scott:tiger@mydsn",
                Dialect.mssql,
                marks=skipif_not_linux,
            ),
            param(
                "mysql://scott:tiger@localhost/foo",
                Dialect.mysql,
                marks=skipif_not_linux,
            ),
            param("oracle://scott:tiger@127.0.0.1:1521/sidname", Dialect.oracle),
            param(
                "postgresql://scott:tiger@localhost/mydatabase",
                Dialect.postgresql,
                marks=skipif_not_linux,
            ),
        ],
    )
    def test_non_sqlite(self, *, url: str, expected: Dialect) -> None:
        assert get_dialect(_create_engine(url)) is expected


class TestGetTable:
    def test_table(self) -> None:
        table = Table("example", MetaData(), Column("id_", Integer, primary_key=True))
        result = get_table(table)
        assert result is table

    def test_mapped_class(self) -> None:
        class Example(declarative_base()):
            __tablename__ = "example"

            id_ = Column(Integer, primary_key=True)

        table = get_table(Example)
        result = get_table(table)
        assert result is Example.__table__

    def test_error(self) -> None:
        with raises(
            GetTableError, match="Object .* must be a Table or mapped class; got .*"
        ):
            _ = get_table(type(None))


class TestInsertItems:
    @given(engine=sqlite_engines(), id_=integers(0, 10))
    def test_pair_of_tuple_and_table(self, *, engine: Engine, id_: int) -> None:
        self._run_test(engine, {id_}, ((id_,), self._table))

    @given(engine=sqlite_engines(), id_=integers(0, 10))
    def test_pair_of_dict_and_table(self, *, engine: Engine, id_: int) -> None:
        self._run_test(engine, {id_}, ({"id_": id_}, self._table))

    @given(engine=sqlite_engines(), ids=sets(integers(0, 10), min_size=1))
    def test_pair_of_lists_of_tuples_and_table(
        self, *, engine: Engine, ids: set[int]
    ) -> None:
        self._run_test(engine, ids, ([((id_,)) for id_ in ids], self._table))

    @given(engine=sqlite_engines(), ids=sets(integers(0, 10), min_size=1))
    def test_pair_of_lists_of_dicts_and_table(
        self, *, engine: Engine, ids: set[int]
    ) -> None:
        self._run_test(engine, ids, ([({"id_": id_}) for id_ in ids], self._table))

    @given(engine=sqlite_engines(), ids=sets(integers(0, 10), min_size=1))
    def test_list_of_pairs_of_tuples_and_tables(
        self, *, engine: Engine, ids: set[int]
    ) -> None:
        self._run_test(engine, ids, [(((id_,), self._table)) for id_ in ids])

    @given(engine=sqlite_engines(), ids=sets(integers(0, 10), min_size=1))
    def test_list_of_pairs_of_dicts_and_tables(
        self, *, engine: Engine, ids: set[int]
    ) -> None:
        self._run_test(engine, ids, [({"id_": id_}, self._table) for id_ in ids])

    @given(
        engine=sqlite_engines(), ids=sets(integers(0, 1000), min_size=10, max_size=100)
    )
    def test_many_items(self, *, engine: Engine, ids: set[int]) -> None:
        self._run_test(engine, ids, [({"id_": id_}, self._table) for id_ in ids])

    @given(engine=sqlite_engines(), id_=integers(0, 10))
    def test_mapped_class(self, *, engine: Engine, id_: int) -> None:
        class Example(declarative_base()):
            __tablename__ = "example"

            id_ = Column(Integer, primary_key=True)

        self._run_test(engine, {id_}, Example(id_=id_))

    @property
    def _table(self) -> Table:
        return Table("example", MetaData(), Column("id_", Integer, primary_key=True))

    def _run_test(self, engine: Engine, ids: set[int], /, *args: Any) -> None:
        ensure_tables_created(engine, self._table)
        insert_items(engine, *args)
        sel = select(self._table.c["id_"])
        with engine.begin() as conn:
            res = conn.execute(sel).scalars().all()
        assert set(res) == ids


class TestInsertItemsCollect:
    @given(id_=integers())
    def test_pair_with_tuple_data(self, *, id_: int) -> None:
        table = Table("example", MetaData(), Column("id", Integer, primary_key=True))
        result = list(_insert_items_collect(((id_,), table)))
        expected = [_InsertionItem(values=(id_,), table=table)]
        assert result == expected

    @given(id_=integers())
    def test_pair_with_dict_data(self, *, id_: int) -> None:
        table = Table("example", MetaData(), Column("id", Integer, primary_key=True))
        result = list(_insert_items_collect(({"id": id_}, table)))
        expected = [_InsertionItem(values={"id": id_}, table=table)]
        assert result == expected

    @given(ids=sets(integers()))
    def test_pair_with_list_of_tuple_data(self, *, ids: set[int]) -> None:
        table = Table("example", MetaData(), Column("id", Integer, primary_key=True))
        result = list(_insert_items_collect(([(id_,) for id_ in ids], table)))
        expected = [_InsertionItem(values=(id_,), table=table) for id_ in ids]
        assert result == expected

    @given(ids=sets(integers()))
    def test_pair_with_list_of_dict_data(self, *, ids: set[int]) -> None:
        table = Table("example", MetaData(), Column("id", Integer, primary_key=True))
        result = list(_insert_items_collect(([{"id": id_} for id_ in ids], table)))
        expected = [_InsertionItem(values={"id": id_}, table=table) for id_ in ids]
        assert result == expected

    @given(ids=sets(integers()))
    def test_list(self, *, ids: set[int]) -> None:
        table = Table("example", MetaData(), Column("id", Integer, primary_key=True))
        result = list(_insert_items_collect([((id_,), table) for id_ in ids]))
        expected = [_InsertionItem(values=(id_,), table=table) for id_ in ids]
        assert result == expected

    @given(ids=sets(integers()))
    def test_set(self, *, ids: set[int]) -> None:
        table = Table("example", MetaData(), Column("id", Integer, primary_key=True))
        result = list(_insert_items_collect({((id_,), table) for id_ in ids}))
        assert {one(r.values) for r in result} == ids

    @given(id_=integers())
    def test_mapped_class(self, *, id_: int) -> None:
        class Example(declarative_base()):
            __tablename__ = "example"

            id_ = Column(Integer, primary_key=True)

        item = Example(id_=id_)
        result = list(_insert_items_collect(item))
        expected = [_InsertionItem(values={"id_": id_}, table=get_table(Example))]
        assert result == expected

    @mark.parametrize(
        "item",
        [
            param((None,), id="tuple length"),
            param((None, None), id="second argument not a table or mapped class"),
            param(None, id="outright invalid"),
        ],
    )
    def test_errors(self, *, item: Any) -> None:
        with raises(_InsertItemsCollectError):
            _ = list(_insert_items_collect(item))

    def test_error_tuple_but_first_argument_invalid(self) -> None:
        table = Table("example", MetaData(), Column("id", Integer, primary_key=True))
        with raises(_InsertItemsCollectError):
            _ = list(_insert_items_collect((None, table)))


class TestInsertItemsCollectIterable:
    @given(ids=sets(integers()))
    def test_list_of_tuples(self, *, ids: set[int]) -> None:
        table = Table("example", MetaData(), Column("id", Integer, primary_key=True))
        result = list(_insert_items_collect_iterable([(id_,) for id_ in ids], table))
        expected = [_InsertionItem(values=(id_,), table=table) for id_ in ids]
        assert result == expected

    @given(ids=sets(integers()))
    def test_list_of_dicts(self, *, ids: set[int]) -> None:
        table = Table("example", MetaData(), Column("id", Integer, primary_key=True))
        result = list(
            _insert_items_collect_iterable([{"id": id_} for id_ in ids], table)
        )
        expected = [_InsertionItem(values={"id": id_}, table=table) for id_ in ids]
        assert result == expected

    def test_error(self) -> None:
        table = Table("example", MetaData(), Column("id", Integer, primary_key=True))
        with raises(_InsertItemsCollectIterableError):
            _ = list(_insert_items_collect_iterable([None], table))


class TestInsertItemsCollectValid:
    @mark.parametrize(
        ("obj", "expected"),
        [
            param(None, False),
            param((1, 2, 3), True),
            param({"a": 1, "b": 2, "c": 3}, True),
            param({1: "a", 2: "b", 3: "c"}, False),
        ],
    )
    def test_main(self, *, obj: Any, expected: bool) -> None:
        result = _insert_items_collect_valid(obj)
        assert result is expected


class TestIsMappedClass:
    def test_mapped_class(self) -> None:
        class Example(declarative_base()):
            __tablename__ = "example"

            id_ = Column(Integer, primary_key=True)

        assert is_mapped_class(Example)

    def test_other(self) -> None:
        assert not is_mapped_class(int)


class TestIsTableOrMappedClass:
    def test_table(self) -> None:
        table = Table("example", MetaData(), Column("id_", Integer, primary_key=True))
        assert is_table_or_mapped_class(table)

    def test_mapped_class(self) -> None:
        class Example(declarative_base()):
            __tablename__ = "example"

            id_ = Column(Integer, primary_key=True)

        assert is_table_or_mapped_class(Example)

    def test_other(self) -> None:
        assert not is_table_or_mapped_class(int)


class TestMappedClassToDict:
    @given(id_=integers())
    def test_main(self, *, id_: int) -> None:
        class Example(declarative_base()):
            __tablename__ = "example"
            id_ = Column(Integer, primary_key=True)

        example = Example(id_=id_)
        result = mapped_class_to_dict(example)
        expected = {"id_": id_}
        assert result == expected

    @given(id_=integers())
    def test_explicitly_named_column(self, *, id_: int) -> None:
        class Example(declarative_base()):
            __tablename__ = "example"
            ID = Column(Integer, primary_key=True, name="id")

        example = Example(ID=id_)
        result = mapped_class_to_dict(example)
        expected = {"id": id_}
        assert result == expected
