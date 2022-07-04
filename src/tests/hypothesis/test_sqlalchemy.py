from pathlib import Path
from typing import Any
from typing import cast

from hypothesis import given
from hypothesis.strategies import DataObject
from hypothesis.strategies import data
from hypothesis.strategies import integers
from hypothesis.strategies import sets
from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import Table
from sqlalchemy import insert
from sqlalchemy import select
from sqlalchemy.engine import Engine
from sqlalchemy.orm import declarative_base

from utilities.hypothesis.sqlalchemy import sqlite_engines


class TestSQLiteEngines:
    @given(engine=sqlite_engines())
    def test_main(self, engine: Engine) -> None:
        assert isinstance(engine, Engine)
        assert (database := engine.url.database) is not None
        assert not Path(database).exists()

    @given(data=data(), ids=sets(integers(0, 100), min_size=1, max_size=10))
    def test_core(self, data: DataObject, ids: set[int]) -> None:
        metadata = MetaData()
        table = Table(
            "example", metadata, Column("id", Integer, primary_key=True)
        )
        engine = data.draw(sqlite_engines(metadata=metadata))
        self._run_test(engine, table, ids)

    @given(data=data(), ids=sets(integers(0, 100), min_size=1, max_size=10))
    def test_orm(self, data: DataObject, ids: set[int]) -> None:
        Base = cast(Any, declarative_base())

        class Example(Base):
            __tablename__ = "example"
            id = Column(Integer, primary_key=True)

        engine = data.draw(sqlite_engines(base=Base))
        self._run_test(engine, Example, ids)

    def _run_test(
        self, engine: Engine, table_or_model: Any, ids: set[int]
    ) -> None:
        with engine.begin() as conn:
            _ = conn.execute(insert(table_or_model), [{"id": id} for id in ids])
            res = conn.execute(select(table_or_model)).all()
        assert {r["id"] for r in res} == ids
