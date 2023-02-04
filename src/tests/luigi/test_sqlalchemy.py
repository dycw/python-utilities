from typing import Any, cast

from hypothesis import given
from luigi import Task
from sqlalchemy import Column, Integer
from sqlalchemy.engine import Engine
from sqlalchemy.orm import declarative_base

from utilities.hypothesis.luigi import namespace_mixins
from utilities.hypothesis.sqlalchemy import sqlite_engines
from utilities.luigi.sqlalchemy import EngineParameter, TableParameter


class TestEngineParameter:
    @given(namespace_mixin=namespace_mixins(), engine=sqlite_engines())
    def test_main(self, namespace_mixin: Any, engine: Engine) -> None:
        class Example(namespace_mixin, Task):
            engine = EngineParameter()

        _ = Example(engine)


class TestTableParameter:
    @given(namespace_mixin=namespace_mixins())
    def test_main(self, namespace_mixin: Any) -> None:
        class ExampleTask(namespace_mixin, Task):
            table = TableParameter()

        class ExampleTable(cast(Any, declarative_base())):
            __tablename__ = "example"

            id_ = Column(Integer, primary_key=True)

        _ = ExampleTask(ExampleTable)
