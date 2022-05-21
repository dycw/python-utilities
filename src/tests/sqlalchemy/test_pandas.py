import datetime as dt
from typing import Any
from typing import cast

from hypothesis import given
from hypothesis.extra.pandas import column
from hypothesis.extra.pandas import data_frames
from hypothesis.extra.pandas import range_indexes
from numpy import inf
from numpy import nan
from pandas import NA
from pandas import DataFrame
from pandas import Series
from pandas import to_datetime
from pytest import mark
from pytest import param
from sqlalchemy import Boolean
from sqlalchemy import Column
from sqlalchemy import Date
from sqlalchemy import DateTime
from sqlalchemy import Float
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy import select
from sqlalchemy.engine import Engine
from sqlalchemy.orm import declarative_base

from dycw_utilities.hypothesis.sqlalchemy import sqlite_engines
from dycw_utilities.numpy import datetime64ns
from dycw_utilities.pandas import Int64
from dycw_utilities.pandas import boolean
from dycw_utilities.pandas import string
from dycw_utilities.sqlalchemy.pandas import insert_dataframe
from dycw_utilities.sqlalchemy.pandas import nativize_column


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


class TestNativizeColumn:
    @mark.parametrize(
        "series, expected",
        [
            param(Series([True, False], dtype=bool), [True, False]),
            param(
                Series([True, False, None], dtype=boolean), [True, False, None]
            ),
        ],
    )
    @mark.parametrize(
        "column", [param(Column(Boolean)), param(Column(Integer))]
    )
    def test_boolean_data(
        self, series: Series, column: Any, expected: list[Any]
    ) -> None:
        res = list(nativize_column(series, column))
        assert res == expected

    @mark.parametrize(
        "series, column, expected",
        [
            param(
                Series([to_datetime("2000-01-01"), NA], dtype=datetime64ns),
                Column(Date),
                [dt.date(2000, 1, 1), None],
            ),
            param(
                Series(
                    [to_datetime("2000-01-01 12:00:00"), NA], dtype=datetime64ns
                ),
                Column(DateTime),
                [dt.datetime(2000, 1, 1, 12), None],
            ),
        ],
    )
    def test_datetime_data(
        self, series: Series, column: Any, expected: list[Any]
    ) -> None:
        res = list(nativize_column(series, column))
        assert res == expected

    @mark.parametrize(
        "series, column, expected",
        [
            param(
                Series([0.0, nan, inf, -inf], dtype=float),
                Column(Float),
                [0.0, None, inf, -inf],
            ),
            param(Series([0], dtype=int), Column(Integer), [0]),
            param(Series([0, NA], dtype=Int64), Column(Integer), [0, None]),
            param(
                Series(["string", NA], dtype=string),
                Column(String),
                ["string", None],
            ),
        ],
    )
    def test_float_int_and_str_data(
        self, series: Series, column: Any, expected: list[Any]
    ) -> None:
        res = list(nativize_column(series, column))
        assert res == expected
