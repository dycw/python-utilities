from hypothesis import given
from sqlalchemy.engine import Engine

from dycw_utilities.hypothesis.sqlalchemy import sqlite_engines


class TestInsertDataFrame:
    @given(engine=sqlite_engines())
    def test_main(self, engine: Engine) -> None:
        pass
