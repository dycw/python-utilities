from __future__ import annotations

from operator import eq
from typing import Any

from hypothesis import given
from hypothesis.strategies import (
    integers,
    sets,
)
from pytest import mark, param, raises
from sqlalchemy import Column, Connection, Engine, Integer, MetaData, Table, select
from sqlalchemy import create_engine as _create_engine
from sqlalchemy.exc import DuplicateColumnError
from sqlalchemy.orm import declarative_base

from utilities._sqlalchemy.common import (
    CheckSeriesAgainstTableColumnError,
    CheckSeriesAgainstTableSchemaError,
    Dialect,
    GetTableError,
    InsertItemsCollectError,
    InsertItemsCollectIterableError,
    _InsertionItem,
    check_selectable_for_duplicate_columns,
    check_series_against_table_column,
    check_series_against_table_schema,
    get_column_names,
    get_columns,
    get_dialect,
    get_table,
    insert_items,
    insert_items_collect,
    insert_items_collect_iterable,
    insert_items_collect_valid,
    is_mapped_class,
    is_table_or_mapped_class,
    mapped_class_to_dict,
    yield_connection,
)
from utilities.hypothesis import (
    sqlite_engines,
)
from utilities.itertools import one
from utilities.pytest import skipif_not_linux
from utilities.sqlalchemy import ensure_tables_created


class TestCheckSelectableForDuplicates:
    def test_error(self) -> None:
        table = Table("example", MetaData(), Column("id", Integer, primary_key=True))
        sel = select(table.c.id, table.c.id)
        with raises(DuplicateColumnError):
            check_selectable_for_duplicate_columns(sel)


class TestCheckSeriesAgainstAgainstTableColumn:
    def test_main(self) -> None:
        schema = {"a": int, "b": float, "c": str}
        result = check_series_against_table_column("b", schema)
        expected = ("b", float)
        assert result == expected

    @mark.parametrize("sr_name", [param("b"), param("B")])
    def test_snake(self, *, sr_name: str) -> None:
        schema = {"A": int, "B": float, "C": str}
        result = check_series_against_table_column(sr_name, schema, snake=True)
        expected = ("B", float)
        assert result == expected

    @mark.parametrize("snake", [param(True), param(False)])
    def test_error_empty(self, *, snake: bool) -> None:
        schema = {"a": int, "b": float, "c": str}
        with raises(CheckSeriesAgainstTableColumnError):
            _ = check_series_against_table_column("value", schema, snake=snake)

    def test_error_non_unique(self) -> None:
        schema = {"a": int, "b": float, "B": float, "c": str}
        with raises(CheckSeriesAgainstTableColumnError):
            _ = check_series_against_table_column("b", schema, snake=True)


class TestCheckSeriesAgainstAgainstTableSchema:
    def test_success(self) -> None:
        table_schema = {"a": int, "b": float, "c": str}
        result = check_series_against_table_schema("b", float, table_schema, eq)
        assert result == "b"

    def test_fail(self) -> None:
        table_schema = {"a": int, "b": float, "c": str}
        with raises(CheckSeriesAgainstTableSchemaError):
            _ = check_series_against_table_schema("b", int, table_schema, eq)


class TestDialect:
    @mark.parametrize("dialect", Dialect)
    def test_max_params(self, *, dialect: Dialect) -> None:
        assert isinstance(dialect.max_params, int)


class TestGetColumnNames:
    def test_table(self) -> None:
        table = Table("example", MetaData(), Column("id_", Integer, primary_key=True))
        self._run_test(table)

    def test_mapped_class(self) -> None:
        class Example(declarative_base()):
            __tablename__ = "example"

            id_ = Column(Integer, primary_key=True)

        self._run_test(Example)

    def _run_test(self, table_or_mapped_class: Table | type[Any], /) -> None:
        assert get_column_names(table_or_mapped_class) == ["id_"]


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
        with raises(GetTableError):
            _ = get_table(type(None))


class TestInsertItems:
    @given(engine=sqlite_engines(), id_=integers(0, 10))
    def test_pair_of_tuple_and_table(self, *, engine: Engine, id_: int) -> None:
        self._run_test(engine, {id_}, ((id_,), self._table))

    @given(engine=sqlite_engines(), id_=integers(0, 10))
    def test_pair_of_dict_and_table(self, *, engine: Engine, id_: int) -> None:
        self._run_test(engine, {id_}, ({"id": id_}, self._table))

    @given(engine=sqlite_engines(), ids=sets(integers(0, 10), max_size=10))
    def test_pair_of_lists_of_tuples_and_table(
        self, *, engine: Engine, ids: set[int]
    ) -> None:
        self._run_test(engine, ids, ([((id_,)) for id_ in ids], self._table))

    @given(engine=sqlite_engines(), ids=sets(integers(0, 10)))
    def test_pair_of_lists_of_dicts_and_table(
        self, *, engine: Engine, ids: set[int]
    ) -> None:
        self._run_test(engine, ids, ([({"id": id_}) for id_ in ids], self._table))

    @given(engine=sqlite_engines(), ids=sets(integers(0, 10)))
    def test_list_of_pairs_of_tuples_and_tables(
        self, *, engine: Engine, ids: set[int]
    ) -> None:
        self._run_test(engine, ids, [(((id_,), self._table)) for id_ in ids])

    @given(engine=sqlite_engines(), ids=sets(integers(0, 10)))
    def test_list_of_pairs_of_dicts_and_tables(
        self, *, engine: Engine, ids: set[int]
    ) -> None:
        self._run_test(engine, ids, [({"id": id_}, self._table) for id_ in ids])

    @given(
        engine=sqlite_engines(),
        ids=sets(integers(0, 10000), min_size=1000, max_size=1000),
    )
    def test_many_items(self, *, engine: Engine, ids: set[int]) -> None:
        self._run_test(engine, ids, [({"id": id_}, self._table) for id_ in ids])

    @given(engine=sqlite_engines(), id_=integers(0, 10))
    def test_mapped_class(self, *, engine: Engine, id_: int) -> None:
        class Example(declarative_base()):
            __tablename__ = "example"

            id = Column(Integer, primary_key=True)  # noqa: A003

        self._run_test(engine, {id_}, Example(id=id_))

    @property
    def _table(self) -> Table:
        return Table("example", MetaData(), Column("id", Integer, primary_key=True))

    def _run_test(self, engine: Engine, ids: set[int], /, *args: Any) -> None:
        ensure_tables_created(engine, self._table)
        insert_items(engine, *args)
        sel = select(self._table.c["id"])
        with engine.begin() as conn:
            res = conn.execute(sel).scalars().all()
        assert set(res) == ids


class TestInsertItemsCollect:
    @given(id_=integers())
    def test_pair_with_tuple_data(self, *, id_: int) -> None:
        table = Table("example", MetaData(), Column("id", Integer, primary_key=True))
        result = list(insert_items_collect(((id_,), table)))
        expected = [_InsertionItem(values=(id_,), table=table)]
        assert result == expected

    @given(id_=integers())
    def test_pair_with_dict_data(self, *, id_: int) -> None:
        table = Table("example", MetaData(), Column("id", Integer, primary_key=True))
        result = list(insert_items_collect(({"id": id_}, table)))
        expected = [_InsertionItem(values={"id": id_}, table=table)]
        assert result == expected

    @given(ids=sets(integers()))
    def test_pair_with_list_of_tuple_data(self, *, ids: set[int]) -> None:
        table = Table("example", MetaData(), Column("id", Integer, primary_key=True))
        result = list(insert_items_collect(([(id_,) for id_ in ids], table)))
        expected = [_InsertionItem(values=(id_,), table=table) for id_ in ids]
        assert result == expected

    @given(ids=sets(integers()))
    def test_pair_with_list_of_dict_data(self, *, ids: set[int]) -> None:
        table = Table("example", MetaData(), Column("id", Integer, primary_key=True))
        result = list(insert_items_collect(([{"id": id_} for id_ in ids], table)))
        expected = [_InsertionItem(values={"id": id_}, table=table) for id_ in ids]
        assert result == expected

    @given(ids=sets(integers()))
    def test_list(self, *, ids: set[int]) -> None:
        table = Table("example", MetaData(), Column("id", Integer, primary_key=True))
        result = list(insert_items_collect([((id_,), table) for id_ in ids]))
        expected = [_InsertionItem(values=(id_,), table=table) for id_ in ids]
        assert result == expected

    @given(ids=sets(integers()))
    def test_set(self, *, ids: set[int]) -> None:
        table = Table("example", MetaData(), Column("id", Integer, primary_key=True))
        result = list(insert_items_collect({((id_,), table) for id_ in ids}))
        assert {one(r.values) for r in result} == ids

    @given(id_=integers())
    def test_mapped_class(self, *, id_: int) -> None:
        class Example(declarative_base()):
            __tablename__ = "example"

            id_ = Column(Integer, primary_key=True)

        item = Example(id_=id_)
        result = list(insert_items_collect(item))
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
        with raises(InsertItemsCollectError):
            _ = list(insert_items_collect(item))

    def test_error_tuple_but_first_argument_invalid(self) -> None:
        table = Table("example", MetaData(), Column("id", Integer, primary_key=True))
        with raises(InsertItemsCollectError):
            _ = list(insert_items_collect((None, table)))


class TestInsertItemsCollectIterable:
    @given(ids=sets(integers()))
    def test_list_of_tuples(self, *, ids: set[int]) -> None:
        table = Table("example", MetaData(), Column("id", Integer, primary_key=True))
        result = list(insert_items_collect_iterable([(id_,) for id_ in ids], table))
        expected = [_InsertionItem(values=(id_,), table=table) for id_ in ids]
        assert result == expected

    @given(ids=sets(integers()))
    def test_list_of_dicts(self, *, ids: set[int]) -> None:
        table = Table("example", MetaData(), Column("id", Integer, primary_key=True))
        result = list(
            insert_items_collect_iterable([{"id": id_} for id_ in ids], table)
        )
        expected = [_InsertionItem(values={"id": id_}, table=table) for id_ in ids]
        assert result == expected

    def test_error(self) -> None:
        table = Table("example", MetaData(), Column("id", Integer, primary_key=True))
        with raises(InsertItemsCollectIterableError):
            _ = list(insert_items_collect_iterable([None], table))


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
        result = insert_items_collect_valid(obj)
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


class TestYieldConnection:
    @given(engine=sqlite_engines())
    def test_engine(self, *, engine: Engine) -> None:
        with yield_connection(engine) as conn:
            assert isinstance(conn, Connection)

    @given(engine=sqlite_engines())
    def test_connection(self, *, engine: Engine) -> None:
        with engine.begin() as conn1, yield_connection(conn1) as conn2:
            assert isinstance(conn2, Connection)
