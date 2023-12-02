from __future__ import annotations

from typing import Any

from hypothesis import given
from hypothesis.strategies import integers, sets, tuples
from luigi import Task
from sqlalchemy import Column, Engine, Integer, MetaData, Table, select
from sqlalchemy.orm import declarative_base

from utilities.hypothesis import namespace_mixins, sqlite_engines
from utilities.luigi import DatabaseTarget, EngineParameter, TableParameter
from utilities.sqlalchemy import insert_items


class TestDatabaseTarget:
    @given(engine=sqlite_engines(), rows=sets(tuples(integers(0, 10), integers(0, 10))))
    def test_main(self, *, engine: Engine, rows: set[tuple[int, int]]) -> None:
        table = Table(
            "example",
            MetaData(),
            Column("id1", Integer, primary_key=True),
            Column("id2", Integer, primary_key=True),
        )
        sel = select(table).where(table.c["id1"] == 0)
        target = DatabaseTarget(sel, engine)
        assert not target.exists()
        insert_items(engine, (rows, table))
        expected = any(first == 0 for first, _ in rows)
        assert target.exists() is expected


class TestEngineParameter:
    @given(engine=sqlite_engines())
    def test_main(self, engine: Engine) -> None:
        param = EngineParameter()
        norm = param.normalize(engine)
        new_engine = param.parse(param.serialize(norm))
        assert new_engine.url == norm.url


class TestTableParameter:
    @given(namespace_mixin=namespace_mixins())
    def test_main(self, namespace_mixin: Any) -> None:
        class ExampleTask(namespace_mixin, Task):
            table = TableParameter()

        class ExampleTable(declarative_base()):
            __tablename__ = "example"

            id_ = Column(Integer, primary_key=True)

        _ = ExampleTask(ExampleTable)
