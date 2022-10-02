from pathlib import Path
from typing import Any
from typing import cast

from hypothesis import given
from hypothesis.strategies import fixed_dictionaries
from hypothesis.strategies import integers
from hypothesis.strategies import lists
from hypothesis.strategies import none
from pytest import mark
from pytest import param
from pytest import raises
from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import Table
from sqlalchemy import select
from sqlalchemy.engine import Engine
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import declarative_base

from utilities.hypothesis.sqlalchemy import sqlite_engines
from utilities.hypothesis.tempfile import temp_paths
from utilities.sqlalchemy import columnwise_max
from utilities.sqlalchemy import columnwise_min
from utilities.sqlalchemy import create_engine
from utilities.sqlalchemy import ensure_table_created
from utilities.sqlalchemy import ensure_table_dropped
from utilities.sqlalchemy import get_column_names
from utilities.sqlalchemy import get_columns
from utilities.sqlalchemy import get_table


class TestColumnwiseMinMax:
    @given(
        values=lists(
            fixed_dictionaries(
                {"x": integers(0, 100) | none(), "y": integers(0, 100) | none()}
            ),
            min_size=1,
            max_size=10,
        ),
        engine=sqlite_engines(),
    )
    def test_main(
        self, values: list[dict[str, int | None]], engine: Engine
    ) -> None:
        table = Table(
            "example",
            MetaData(),
            Column("id", Integer, primary_key=True, autoincrement=True),
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
            Column("id", Integer, primary_key=True, autoincrement=True),
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
            "example", MetaData(), Column("id", Integer, primary_key=True)
        )
        self._run_test(table, engine, runs)

    @given(engine=sqlite_engines())
    @mark.parametrize("runs", [param(1), param(2)])
    def test_orm(self, engine: Engine, runs: int) -> None:
        class Example(cast(Any, declarative_base())):
            __tablename__ = "example"
            id = Column(Integer, primary_key=True)

        self._run_test(Example, engine, runs)

    def _run_test(
        self, table_or_model: Any, engine: Engine, runs: int, /
    ) -> None:
        sel = get_table(table_or_model).select()
        with raises(
            OperationalError, match="no such table"
        ), engine.begin() as conn:
            _ = conn.execute(sel).all()

        for _ in range(runs):
            ensure_table_created(table_or_model, engine)

        with engine.begin() as conn:
            _ = conn.execute(sel).all()


class TestEnsureTableDropped:
    @given(engine=sqlite_engines())
    @mark.parametrize("runs", [param(1), param(2)])
    def test_core(self, engine: Engine, runs: int) -> None:
        table = Table(
            "example", MetaData(), Column("id", Integer, primary_key=True)
        )
        self._run_test(table, engine, runs)

    @given(engine=sqlite_engines())
    @mark.parametrize("runs", [param(1), param(2)])
    def test_orm(self, engine: Engine, runs: int) -> None:
        class Example(cast(Any, declarative_base())):
            __tablename__ = "example"
            id = Column(Integer, primary_key=True)

        self._run_test(Example, engine, runs)

    def _run_test(
        self, table_or_model: Any, engine: Engine, runs: int, /
    ) -> None:
        table = get_table(table_or_model)
        sel = table.select()
        with engine.begin() as conn:
            table.create(conn)
            _ = conn.execute(sel).all()

        for _ in range(runs):
            ensure_table_dropped(table_or_model, engine)

        with raises(
            OperationalError, match="no such table"
        ), engine.begin() as conn:
            _ = conn.execute(sel).all()


class TestGetColumnNames:
    def test_core(self) -> None:
        table = Table(
            "example", MetaData(), Column("id", Integer, primary_key=True)
        )
        self._run_test(table)

    def test_orm(self) -> None:
        class Example(cast(Any, declarative_base())):
            __tablename__ = "example"
            id = Column(Integer, primary_key=True)

        self._run_test(Example)

    def _run_test(self, table_or_model: Any, /) -> None:
        assert get_column_names(table_or_model) == ["id"]


class TestGetColumns:
    def test_core(self) -> None:
        table = Table(
            "example", MetaData(), Column("id", Integer, primary_key=True)
        )
        self._run_test(table)

    def test_orm(self) -> None:
        class Example(cast(Any, declarative_base())):
            __tablename__ = "example"
            id = Column(Integer, primary_key=True)

        self._run_test(Example)

    def _run_test(self, table_or_model: Any, /) -> None:
        columns = get_columns(table_or_model)
        assert isinstance(columns, list)
        assert len(columns) == 1
        assert isinstance(columns[0], Column)


class TestGetTable:
    def test_core(self) -> None:
        table = Table("example", MetaData(), Column("id", primary_key=True))
        assert get_table(table) is table

    def test_orm(self) -> None:
        class Example(cast(Any, declarative_base())):
            __tablename__ = "example"
            id = Column(Integer, primary_key=True)

        table = get_table(Example)
        assert isinstance(table, Table)
        assert table.name == "example"
