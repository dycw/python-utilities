from __future__ import annotations

from typing import Any

from hypothesis import given
from hypothesis.strategies import DataObject, data, integers, sets
from sqlalchemy import Column, Engine, Integer, MetaData, Table, select
from sqlalchemy.orm import declarative_base

from utilities._hypothesis.sqlalchemy import sqlite_engines
from utilities.pathvalidate import valid_path
from utilities.sqlalchemy import get_table, insert_items


class TestSQLiteEngines:
    @given(engine=sqlite_engines())
    def test_main(self, *, engine: Engine) -> None:
        assert isinstance(engine, Engine)
        database = engine.url.database
        assert database is not None
        assert not valid_path(database).exists()

    @given(data=data(), ids=sets(integers(0, 10)))
    def test_table(self, *, data: DataObject, ids: set[int]) -> None:
        metadata = MetaData()
        table = Table("example", metadata, Column("id_", Integer, primary_key=True))
        engine = data.draw(sqlite_engines(metadata=metadata))
        self._run_test(engine, table, ids)

    @given(data=data(), ids=sets(integers(0, 10)))
    def test_mapped_class(self, *, data: DataObject, ids: set[int]) -> None:
        Base = declarative_base()  # noqa: N806

        class Example(Base):
            __tablename__ = "example"

            id_ = Column(Integer, primary_key=True)

        engine = data.draw(sqlite_engines(base=Base))
        self._run_test(engine, Example, ids)

    def _run_test(
        self, engine: Engine, table_or_mapped_class: Table | type[Any], ids: set[int], /
    ) -> None:
        insert_items(engine, ([(id_,) for id_ in ids], table_or_mapped_class))
        sel = select(get_table(table_or_mapped_class).c["id_"])
        with engine.begin() as conn:
            res = conn.execute(sel).scalars().all()
        assert set(res) == ids
