from typing import Any
from typing import cast

from hypothesis import given
from hypothesis.extra.pandas import column
from hypothesis.extra.pandas import data_frames
from hypothesis.extra.pandas import range_indexes
from pandas import DataFrame
from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import select
from sqlalchemy.engine import Engine
from sqlalchemy.orm import declarative_base

from dycw_utilities.hypothesis.sqlalchemy import sqlite_engines
from dycw_utilities.sqlalchemy.pandas import insert_dataframe


class TestInsertDataFrame:
    @given(
        df=data_frames(
            [column(name="id", dtype=int)],  # type: ignore
            index=range_indexes(min_size=1, max_size=10),
        ),
        engine=sqlite_engines(),
    )
    def test_main(self, df: DataFrame, engine: Engine) -> None:
        Base = cast(Any, declarative_base())

        class Example(Base):
            __tablename__ = "example"

            id = Column(Integer, primary_key=True)

        with engine.begin() as conn:
            Base.metadata.create_all(conn)

        insert_dataframe(df, Example, engine)
        with engine.begin() as conn:
            res = conn.execute(select(Example)).all()
        assert len(res) == len(df)
