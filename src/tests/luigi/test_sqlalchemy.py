from typing import Any
from typing import cast

from hypothesis import given
from luigi import Task
from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy.engine import Engine
from sqlalchemy.orm import declarative_base

from utilities.hypothesis.luigi import task_namespaces
from utilities.hypothesis.sqlalchemy import sqlite_engines
from utilities.luigi.sqlalchemy import EngineParameter
from utilities.luigi.sqlalchemy import TableParameter


class TestEngineParameter:
    @given(namespace=task_namespaces(), engine=sqlite_engines())
    def test_main(self, namespace: str, engine: Engine) -> None:
        class Example(Task):
            task_namespace = namespace

            engine = EngineParameter()

        _ = Example(engine)


class TestTableParameter:
    @given(namespace=task_namespaces())
    def test_main(self, namespace: str) -> None:
        class ExampleTask(Task):
            task_namespace = namespace

            table = TableParameter()

        class ExampleTable(cast(Any, declarative_base())):
            __tablename__ = "example"

            id = Column(Integer, primary_key=True)

        _ = ExampleTask(ExampleTable)
