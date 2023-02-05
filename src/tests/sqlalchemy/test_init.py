from pathlib import Path
from typing import Any, Optional, cast

from hypothesis import given
from hypothesis.strategies import (
    DataObject,
    data,
    fixed_dictionaries,
    integers,
    lists,
    none,
    sampled_from,
    sets,
)
from hypothesis_sqlalchemy.sample import table_records_lists
from pytest import mark, param, raises
from sqlalchemy import Column, Integer, MetaData, Table, func, insert, select
from sqlalchemy import create_engine as _create_engine
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.exc import DatabaseError, NoSuchTableError
from sqlalchemy.orm import declarative_base

from utilities.hypothesis import temp_paths
from utilities.hypothesis.sqlalchemy import sqlite_engines
from utilities.sqlalchemy import (
    TableAlreadyExistsError,
    columnwise_max,
    columnwise_min,
    create_engine,
    ensure_table_created,
    ensure_table_dropped,
    get_column_names,
    get_columns,
    get_dialect,
    get_table,
    get_table_name,
    model_to_dict,
    redirect_to_no_such_table_error,
    redirect_to_table_already_exists_error,
    yield_connection,
    yield_in_clause_rows,
)


class TestColumnwiseMinMax:
    @given(
        values=lists(
            fixed_dictionaries(
                {
                    "x": integers(0, 100) | none(),
                    "y": integers(0, 100) | none(),
                },
            ),
            min_size=1,
            max_size=10,
        ),
        engine=sqlite_engines(),
    )
    def test_main(
        self,
        values: list[dict[str, Optional[int]]],
        engine: Engine,
    ) -> None:
        table = Table(
            "example",
            MetaData(),
            Column("id_", Integer, primary_key=True, autoincrement=True),
            Column("x", Integer),
            Column("y", Integer),
        )
        ensure_table_created(table, engine)
        with engine.begin() as conn:
            _ = conn.execute(table.insert(), values)
            res = conn.execute(table.select()).all()

        sel = select(
            table.c.x,
            table.c.y,
            columnwise_min(table.c.x, table.c.y).label("min_xy"),
            columnwise_max(table.c.x, table.c.y).label("max_xy"),
        )
        with engine.begin() as conn:
            res = conn.execute(sel).all()

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

    @given(engine=sqlite_engines())
    def test_label(self, engine: Engine) -> None:
        table = Table(
            "example",
            MetaData(),
            Column("id_", Integer, primary_key=True, autoincrement=True),
            Column("x", Integer),
        )
        ensure_table_created(table, engine)

        sel = select(columnwise_min(table.c.x, table.c.x))
        with engine.begin() as conn:
            _ = conn.execute(sel).all()


class TestCreateEngine:
    @given(temp_path=temp_paths())
    def test_main(self, temp_path: Path) -> None:
        engine = create_engine("sqlite", database=temp_path.name)
        assert isinstance(engine, Engine)


class TestEnsureTableCreated:
    @given(engine=sqlite_engines())
    @mark.parametrize("runs", [param(1), param(2)])
    def test_core(self, engine: Engine, runs: int) -> None:
        table = Table(
            "example",
            MetaData(),
            Column("id_", Integer, primary_key=True),
        )
        self._run_test(table, engine, runs)

    @given(engine=sqlite_engines())
    @mark.parametrize("runs", [param(1), param(2)])
    def test_orm(self, engine: Engine, runs: int) -> None:
        class Example(cast(Any, declarative_base())):
            __tablename__ = "example"

            id_ = Column(Integer, primary_key=True)

        self._run_test(Example, engine, runs)

    def _run_test(
        self,
        table_or_model: Any,
        engine: Engine,
        runs: int,
        /,
    ) -> None:
        sel = get_table(table_or_model).select()
        with raises(NoSuchTableError), engine.begin() as conn:
            try:
                _ = conn.execute(sel).all()
            except DatabaseError as error:
                redirect_to_no_such_table_error(engine, error)
        for _ in range(runs):
            ensure_table_created(table_or_model, engine)
        with engine.begin() as conn:
            _ = conn.execute(sel).all()


class TestEnsureTableDropped:
    @given(engine=sqlite_engines())
    @mark.parametrize("runs", [param(1), param(2)])
    def test_core(self, engine: Engine, runs: int) -> None:
        table = Table(
            "example",
            MetaData(),
            Column("id_", Integer, primary_key=True),
        )
        self._run_test(table, engine, runs)

    @given(engine=sqlite_engines())
    @mark.parametrize("runs", [param(1), param(2)])
    def test_orm(self, engine: Engine, runs: int) -> None:
        class Example(cast(Any, declarative_base())):
            __tablename__ = "example"

            id_ = Column(Integer, primary_key=True)

        self._run_test(Example, engine, runs)

    def _run_test(
        self,
        table_or_model: Any,
        engine: Engine,
        runs: int,
        /,
    ) -> None:
        table = get_table(table_or_model)
        sel = table.select()
        with engine.begin() as conn:
            table.create(conn)
            _ = conn.execute(sel).all()
        for _ in range(runs):
            ensure_table_dropped(table_or_model, engine)
        with raises(NoSuchTableError), engine.begin() as conn:
            try:
                _ = conn.execute(sel).all()
            except DatabaseError as error:
                redirect_to_no_such_table_error(engine, error)


class TestGetColumnNames:
    def test_core(self) -> None:
        table = Table(
            "example",
            MetaData(),
            Column("id_", Integer, primary_key=True),
        )
        self._run_test(table)

    def test_orm(self) -> None:
        class Example(cast(Any, declarative_base())):
            __tablename__ = "example"

            id_ = Column(Integer, primary_key=True)

        self._run_test(Example)

    def _run_test(self, table_or_model: Any, /) -> None:
        assert get_column_names(table_or_model) == ["id_"]


class TestGetColumns:
    def test_core(self) -> None:
        table = Table(
            "example",
            MetaData(),
            Column("id", Integer, primary_key=True),
        )
        self._run_test(table)

    def test_orm(self) -> None:
        class Example(cast(Any, declarative_base())):
            __tablename__ = "example"

            id_ = Column(Integer, primary_key=True)

        self._run_test(Example)

    def _run_test(self, table_or_model: Any, /) -> None:
        columns = get_columns(table_or_model)
        assert isinstance(columns, list)
        assert len(columns) == 1
        assert isinstance(columns[0], Column)


class TestGetDialect:
    @given(engine=sqlite_engines())
    def test_sqlite(self, engine: Engine) -> None:
        assert get_dialect(engine) == "sqlite"

    @mark.parametrize(
        ("url", "expected"),
        [
            param("mssql+pyodbc://scott:tiger@mydsn", "mssql"),
            param("mysql://scott:tiger@localhost/foo", "mysql"),
            param("oracle://scott:tiger@127.0.0.1:1521/sidname", "oracle"),
            param(
                "postgresql://scott:tiger@localhost/mydatabase",
                "postgresql",
            ),
        ],
    )
    def test_non_sqlite(self, url: str, expected: str) -> None:
        assert get_dialect(_create_engine(url)) == expected


class TestGetTable:
    def test_core(self) -> None:
        table = Table(
            "example",
            MetaData(),
            Column("id_", Integer, primary_key=True),
        )
        result = get_table(table)
        assert result is table

    def test_orm(self) -> None:
        class Example(cast(Any, declarative_base())):
            __tablename__ = "example"

            id_ = Column(Integer, primary_key=True)

        table = get_table(Example)
        result = get_table(table)
        assert result is Example.__table__


class TestGetTableName:
    def test_core(self) -> None:
        table = Table(
            "example",
            MetaData(),
            Column("id_", Integer, primary_key=True),
        )
        result = get_table_name(table)
        expected = "example"
        assert result == expected

    def test_orm(self) -> None:
        class Example(cast(Any, declarative_base())):
            __tablename__ = "example"

            id_ = Column(Integer, primary_key=True)

        result = get_table_name(Example)
        expected = "example"
        assert result == expected


class TestModelToDict:
    @given(id_=integers())
    def test_main(self, id_: int) -> None:
        class Example(cast(Any, declarative_base())):
            __tablename__ = "example"
            id_ = Column(Integer, primary_key=True)

        example = Example(id_=id_)
        assert model_to_dict(example) == {"id_": id_}

    @given(id_=integers())
    def test_explicitly_named_column(self, id_: int) -> None:
        class Example(cast(Any, declarative_base())):
            __tablename__ = "example"
            ID = Column(Integer, primary_key=True, name="id")

        example = Example(ID=id_)
        assert model_to_dict(example) == {"id": id_}


class TestRedirectToNoSuchTableError:
    @given(engine=sqlite_engines())
    def test_direct(self, engine: Engine) -> None:
        table = Table(
            "example",
            MetaData(),
            Column("id", Integer, primary_key=True),
        )
        with raises(NoSuchTableError), engine.begin() as conn:
            try:
                _ = conn.execute(select(table))
            except DatabaseError as error:
                redirect_to_no_such_table_error(engine, error)


class TestRedirectTableAlreadyExistsError:
    @given(engine=sqlite_engines())
    def test_direct(self, engine: Engine) -> None:
        table = Table(
            "example",
            MetaData(),
            Column("id", Integer, primary_key=True),
        )
        with engine.begin() as conn:
            _ = table.create(conn)
        with raises(TableAlreadyExistsError), engine.begin() as conn:
            try:
                _ = table.create(conn)
            except DatabaseError as error:
                redirect_to_table_already_exists_error(engine, error)


class TestYieldConnection:
    @given(engine=sqlite_engines())
    def test_engine(self, engine: Engine) -> None:
        with yield_connection(engine) as conn:
            assert isinstance(conn, Connection)

    @given(engine=sqlite_engines())
    def test_connection(self, engine: Engine) -> None:
        with engine.begin() as conn1, yield_connection(conn1) as conn2:
            assert isinstance(conn2, Connection)


class TestYieldInClauseRows:
    @given(
        data=data(),
        engine=sqlite_engines(),
        chunk_size=integers(1, 10) | none(),
    )
    def test_main(
        self,
        data: DataObject,
        engine: Engine,
        chunk_size: Optional[int],
    ) -> None:
        table = Table(
            "example",
            MetaData(),
            Column("id", Integer, primary_key=True),
        )
        rows = data.draw(table_records_lists(table, min_size=1))
        num_rows = len(rows)
        with engine.begin() as conn:
            table.create(conn)
            _ = conn.execute(insert(table).values(rows))
            assert (
                conn.execute(select(func.count()).select_from(table)).scalar()
                == num_rows
            )
        row_vals = [row[0] for row in rows]
        values = data.draw(sets(sampled_from(row_vals)))
        result = list(
            yield_in_clause_rows(
                select(table.c.id),
                table.c.id,
                values,
                engine,
                chunk_size=chunk_size,
            ),
        )
        assert len(result) == len(values)
