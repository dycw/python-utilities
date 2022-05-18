from typing import Any
from typing import cast

from hypothesis import given
from pytest import mark
from pytest import param
from pytest import raises
from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import Table
from sqlalchemy.engine import Engine
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import declarative_base

from dycw_utilities.hypothesis.sqlalchemy import sqlite_engines
from dycw_utilities.hypothesis.tempfile import temp_dirs
from dycw_utilities.sqlalchemy import create_engine
from dycw_utilities.sqlalchemy import ensure_table_created
from dycw_utilities.sqlalchemy import ensure_table_dropped
from dycw_utilities.sqlalchemy import get_column_names
from dycw_utilities.sqlalchemy import get_columns
from dycw_utilities.sqlalchemy import get_table
from dycw_utilities.tempfile import TemporaryDirectory


class TestCreateEngine:
    @given(temp_dir=temp_dirs())
    def test_main(self, temp_dir: TemporaryDirectory) -> None:
        engine = create_engine("sqlite", database=temp_dir.name.as_posix())
        assert isinstance(engine, Engine)


class TestEnsureTableCreated:
    @given(engine=sqlite_engines())
    @mark.parametrize("runs", [param(1), param(2)])
    def test_core(self, engine: Engine, runs: int) -> None:
        table = Table("example", MetaData(), Column("id", Integer))
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
        table = Table("example", MetaData(), Column("id", Integer))
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
        table = Table("example", MetaData(), Column("id", Integer))
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
        table = Table("example", MetaData(), Column("id", Integer))
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
