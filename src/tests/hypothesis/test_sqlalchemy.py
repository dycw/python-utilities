from typing import Any
from typing import cast

from hypothesis import given
from hypothesis.strategies import DataObject
from hypothesis.strategies import data
from hypothesis.strategies import integers
from hypothesis.strategies import sets
from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session
from sqlalchemy.orm import declarative_base

from dycw_utilities.hypothesis.sqlalchemy import sqlite_engines


class TestSQLiteEngines:
    @given(engine=sqlite_engines())
    def test_main(self, engine: Engine) -> None:
        assert isinstance(engine, Engine)

    @given(data=data(), values=sets(integers(0, 100), max_size=10))
    def test_post_init(self, data: DataObject, values: set[int]) -> None:
        Base = cast(Any, declarative_base())

        class Example(Base):
            __tablename__ = "example"

            id = Column(Integer, primary_key=True)
            value = Column(Integer)

        def post_init(engine: Engine, /) -> None:
            with engine.begin() as conn:
                Base.metadata.create_all(conn)

        engine = data.draw(sqlite_engines(post_init=post_init))
        assert isinstance(engine, Engine)

        with Session(engine) as session:
            rows = [Example(value=value) for value in values]
            session.add_all(rows)
            session.commit()

        with Session(engine) as session:
            res = session.query(Example).all()
        assert {r.value for r in res} == values
