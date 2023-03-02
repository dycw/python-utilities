from typing import Any, cast

from hypothesis import given
from hypothesis.strategies import DataObject, data, integers
from hypothesis_sqlalchemy.sample import table_records_lists
from luigi import Task
from sqlalchemy import Column, Engine, Integer, MetaData, Table, insert, select
from sqlalchemy.orm import declarative_base

from utilities.hypothesis.luigi import namespace_mixins
from utilities.hypothesis.sqlalchemy import sqlite_engines
from utilities.luigi.sqlalchemy import DatabaseTarget, EngineParameter, TableParameter


class TestDatabaseTarget:
    @given(data=data(), engine=sqlite_engines())
    def test_main(self, data: DataObject, engine: Engine) -> None:
        table = Table(
            "example",
            MetaData(),
            Column("id1", Integer, primary_key=True),
            Column("id2", Integer, primary_key=True),
        )
        sel = select(table).where(table.c.id1 == 0)
        target = DatabaseTarget(sel, engine)
        assert not target.exists()
        rows = data.draw(
            table_records_lists(table, id1=integers(0, 10), min_size=1, max_size=10)
        )
        with engine.begin() as conn:
            table.create(conn)
            _ = conn.execute(insert(table).values(rows))
        expected = any(row[0] == 0 for row in rows)
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

        class ExampleTable(cast(Any, declarative_base())):
            __tablename__ = "example"

            id_ = Column(Integer, primary_key=True)

        _ = ExampleTask(ExampleTable)
