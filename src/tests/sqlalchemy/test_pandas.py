import datetime as dt
from typing import Any, cast

import sqlalchemy
from hypothesis import assume, given
from hypothesis.extra.pandas import column, data_frames, range_indexes
from hypothesis.strategies import integers, sets
from numpy import inf, nan
from pandas import NA, DataFrame, Series, to_datetime
from pytest import mark, param, raises
from sqlalchemy import (
    Boolean,
    Column,
    Date,
    DateTime,
    Float,
    Integer,
    String,
    select,
)
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, declarative_base

from utilities.datetime import UTC
from utilities.hypothesis.sqlalchemy import sqlite_engines
from utilities.numpy import datetime64ns
from utilities.pandas import Int64, boolean, string
from utilities.sqlalchemy import get_table
from utilities.sqlalchemy.pandas import (
    _get_dtype,
    _nativize_column,
    insert_dataframe,
    read_dataframe,
)


class TestGetDType:
    @mark.parametrize(
        ("column", "expected"),
        [
            param(Column(Boolean), boolean),
            param(Column(Date), datetime64ns),
            param(Column(DateTime), datetime64ns),
            param(Column(Float), float),
            param(Column(Integer), Int64),
            param(Column(String), string),
            param(Column(sqlalchemy.DECIMAL), float),
        ],
    )
    def test_main(self, column: Any, expected: Any) -> None:
        assert _get_dtype(column) == expected


class TestInsertDataFrame:
    @given(
        df=data_frames(
            [cast(Any, column)(name="id_", dtype=int)],
            index=range_indexes(max_size=10),
        ),
        engine=sqlite_engines(),
    )
    def test_main(self, df: DataFrame, engine: Engine) -> None:
        _ = assume(not df["id_"].duplicated().any())

        class Example(cast(Any, declarative_base())):
            __tablename__ = "example"
            id_ = Column(Integer, primary_key=True)

        with engine.begin() as conn:
            get_table(Example).create(conn)

        insert_dataframe(df, Example, engine)

        with engine.begin() as conn:
            res = conn.execute(select(Example)).all()
        assert len(res) == len(df)


class TestNativizeColumn:
    @mark.parametrize(
        ("series", "expected"),
        [
            param(Series([True, False], dtype=bool), [True, False]),
            param(
                Series([True, False, None], dtype=boolean),
                [True, False, None],
            ),
        ],
    )
    @mark.parametrize(
        "column",
        [param(Column(Boolean)), param(Column(Integer))],
    )
    def test_boolean_data(
        self,
        series: Series,
        column: Any,
        expected: list[Any],
    ) -> None:
        res = list(_nativize_column(series, column))
        assert res == expected

    @mark.parametrize(
        ("series", "column", "expected"),
        [
            param(
                Series([to_datetime("2000-01-01"), NA], dtype=datetime64ns),
                Column(Date),
                [dt.date(2000, 1, 1), None],
            ),
            param(
                Series(
                    [to_datetime("2000-01-01 12:00:00"), NA],
                    dtype=datetime64ns,
                ),
                Column(DateTime),
                [dt.datetime(2000, 1, 1, 12, tzinfo=UTC), None],
            ),
        ],
    )
    def test_datetime_data(
        self,
        series: Series,
        column: Any,
        expected: list[Any],
    ) -> None:
        res = list(_nativize_column(series, column))
        assert res == expected

    @mark.parametrize(
        ("series", "column", "expected"),
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
        self,
        series: Series,
        column: Any,
        expected: list[Any],
    ) -> None:
        res = list(_nativize_column(series, column))
        assert res == expected

    def test_error(self) -> None:
        series = Series([True, False], dtype=bool)
        column = Column(String)
        with raises(TypeError, match="Invalid types: .*"):
            _ = list(_nativize_column(series, column))


class TestReadDataFrame:
    @given(ids=sets(integers(0, 100), max_size=10), engine=sqlite_engines())
    def test_main(self, ids: set[int], engine: Engine) -> None:
        class Example(cast(Any, declarative_base())):
            __tablename__ = "example"

            id_ = Column(Integer, primary_key=True)

        with engine.begin() as conn:
            get_table(Example).create(conn)
        with Session(engine) as sess:
            sess.add_all([Example(id_=id_) for id_ in ids])
            sess.commit()

        df = read_dataframe(select(Example), engine)
        assert len(df) == len(ids)
        assert dict(df.dtypes) == {"id_": Int64}
