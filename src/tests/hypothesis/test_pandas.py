from collections.abc import Hashable
from typing import cast

from hypothesis import given
from hypothesis.strategies import DataObject, booleans, data, integers, none
from pandas import Index, Timestamp
from pandas.testing import assert_index_equal

from utilities.hypothesis import text_ascii
from utilities.hypothesis.numpy import int64s
from utilities.hypothesis.pandas import (
    dates_pd,
    datetimes_pd,
    indexes,
    int_indexes,
    str_indexes,
)
from utilities.pandas import string


class TestDatesPd:
    @given(data=data())
    def test_main(self, data: DataObject) -> None:
        date = data.draw(dates_pd())
        _ = Timestamp(date)


class TestDatetimesPd:
    @given(data=data())
    def test_main(self, data: DataObject) -> None:
        datetime = data.draw(datetimes_pd())
        _ = Timestamp(datetime)


class TestIndexes:
    @given(
        data=data(),
        n=integers(0, 10),
        unique=booleans(),
        name=text_ascii() | none(),
        sort=booleans(),
    )
    def test_generic(
        self,
        data: DataObject,
        n: int,
        unique: bool,
        name: Hashable,
        sort: bool,
    ) -> None:
        index = data.draw(
            indexes(
                elements=int64s(),
                dtype=int,
                n=n,
                unique=unique,
                name=name,
                sort=sort,
            ),
        )
        assert len(index) == n
        if unique:
            assert not index.duplicated().any()
        assert index.name == name
        if sort:
            assert_index_equal(index, cast(Index, index.sort_values()))

    @given(
        data=data(),
        n=integers(0, 10),
        unique=booleans(),
        name=text_ascii() | none(),
        sort=booleans(),
    )
    def test_int(
        self,
        data: DataObject,
        n: int,
        unique: bool,
        name: Hashable,
        sort: bool,
    ) -> None:
        index = data.draw(
            int_indexes(n=n, unique=unique, name=name, sort=sort),
        )
        assert index.dtype == int
        assert len(index) == n
        if unique:
            assert not index.duplicated().any()
        assert index.name == name
        if sort:
            assert_index_equal(index, cast(Index, index.sort_values()))

    @given(
        data=data(),
        n=integers(0, 10),
        unique=booleans(),
        name=text_ascii() | none(),
        sort=booleans(),
    )
    def test_str(
        self,
        data: DataObject,
        n: int,
        unique: bool,
        name: Hashable,
        sort: bool,
    ) -> None:
        index = data.draw(
            str_indexes(n=n, unique=unique, name=name, sort=sort),
        )
        assert index.dtype == string
        assert len(index) == n
        if unique:
            assert not index.duplicated().any()
        assert index.name == name
        if sort:
            assert_index_equal(index, cast(Index, index.sort_values()))
