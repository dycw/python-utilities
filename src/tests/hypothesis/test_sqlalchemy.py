from pathlib import Path
from typing import Any, cast

from hypothesis import given
from hypothesis.strategies import DataObject, data, integers, sets
from sqlalchemy import Column, Engine, Integer, MetaData, Table, insert, select
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
        table = Table("example", metadata, Column("id_", Integer, primary_key=True))
        engine = data.draw(sqlite_engines(metadata=metadata))
        self._run_test(engine, table, ids)

    @given(data=data(), ids=sets(integers(0, 100), min_size=1, max_size=10))
    def test_orm(self, data: DataObject, ids: set[int]) -> None:
        Base = cast(Any, declarative_base())  # noqa: N806

        class Example(Base):
            __tablename__ = "example"

            id_ = Column(Integer, primary_key=True)

        engine = data.draw(sqlite_engines(base=Base))
        self._run_test(engine, Example, ids)

    def _run_test(self, engine: Engine, table_or_model: Any, ids: set[int]) -> None:
        with engine.begin() as conn:
            _ = conn.execute(insert(table_or_model), [{"id_": id_} for id_ in ids])
            res = conn.execute(select(table_or_model)).all()
        assert {r.id_ for r in res} == ids
