import datetime as dt
from collections.abc import Hashable
from typing import Any
from typing import cast

from hypothesis import assume
from hypothesis import given
from hypothesis.strategies import DataObject
from hypothesis.strategies import booleans
from hypothesis.strategies import data
from hypothesis.strategies import dates
from hypothesis.strategies import integers
from hypothesis.strategies import none
from pandas import Index
from pandas import Timestamp
from pandas.testing import assert_index_equal

from utilities.hypothesis import datetimes_utc
from utilities.hypothesis import text_ascii
from utilities.hypothesis.numpy import int64s
from utilities.hypothesis.pandas import dates_pd
from utilities.hypothesis.pandas import datetimes_pd
from utilities.hypothesis.pandas import indexes
from utilities.hypothesis.pandas import int_indexes
from utilities.hypothesis.pandas import str_indexes
from utilities.hypothesis.pandas import timestamps
from utilities.pandas import TIMESTAMP_MAX_AS_DATE
from utilities.pandas import TIMESTAMP_MAX_AS_DATETIME
from utilities.pandas import TIMESTAMP_MIN_AS_DATE
from utilities.pandas import TIMESTAMP_MIN_AS_DATETIME
from utilities.pandas import string


class TestDatesPd:
    @given(
        data=data(),
        min_value=dates(min_value=TIMESTAMP_MIN_AS_DATE),
        max_value=dates(max_value=TIMESTAMP_MAX_AS_DATE),
    )
    def test_main(
        self,
        data: DataObject,
        min_value: dt.date,
        max_value: dt.date,
    ) -> None:
        _ = assume(min_value <= max_value)
        date = data.draw(dates_pd(min_value=min_value, max_value=max_value))
        _ = Timestamp(date)
        assert min_value <= date <= max_value


class TestDatetimesPd:
    @given(
        data=data(),
        min_value=datetimes_utc(min_value=TIMESTAMP_MIN_AS_DATETIME),
        max_value=datetimes_utc(max_value=TIMESTAMP_MAX_AS_DATETIME),
    )
    def test_main(
        self,
        data: DataObject,
        min_value: dt.datetime,
        max_value: dt.datetime,
    ) -> None:
        _ = assume(min_value <= max_value)
        datetime = data.draw(
            datetimes_pd(min_value=min_value, max_value=max_value),
        )
        _ = Timestamp(datetime)
        assert min_value <= datetime <= max_value


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


class TestTimestamps:
    @given(
        data=data(),
        min_value=datetimes_utc(min_value=TIMESTAMP_MIN_AS_DATETIME),
        max_value=datetimes_utc(max_value=TIMESTAMP_MAX_AS_DATETIME),
        allow_nanoseconds=booleans(),
    )
    def test_main(
        self,
        data: DataObject,
        min_value: dt.datetime,
        max_value: dt.datetime,
        allow_nanoseconds: bool,
    ) -> None:
        _ = assume(min_value <= max_value)
        timestamp = data.draw(
            timestamps(
                min_value=min_value,
                max_value=max_value,
                allow_nanoseconds=allow_nanoseconds,
            ),
        )
        assert min_value <= timestamp <= max_value
        if not allow_nanoseconds:
            assert cast(Any, timestamp).nanosecond == 0
