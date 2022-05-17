from hypothesis import given
from hypothesis.strategies import DataObject
from hypothesis.strategies import data
from sqlalchemy.engine import Engine

from dycw_utilities.hypothesis.sqlalchemy import sqlite_engines
from dycw_utilities.tempfile import TemporaryDirectory


class TestSQLiteEngines:
    @given(engine=sqlite_engines())
    def test_main(self, engine: Engine) -> None:
        assert isinstance(engine, Engine)

    @given(data=data())
    def test_fixed_path(self, data: DataObject) -> None:
        with TemporaryDirectory() as temp:
            engine = data.draw(sqlite_engines(path=temp))
        assert isinstance(engine, Engine)
        assert engine.url.database == temp.as_posix()
