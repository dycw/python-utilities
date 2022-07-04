import datetime as dt
from typing import cast

from hypothesis import given
from pandas import Timestamp
from pandas import to_datetime

from utilities.hypothesis.pandas import dates_pd
from utilities.hypothesis.pandas import datetimes_pd


class TestDatesPd:
    @given(date=dates_pd())
    def test_main(self, date: dt.date) -> None:
        assert isinstance(to_datetime(cast(Timestamp, date)), dt.date)


class TestDatetimesPd:
    @given(date=datetimes_pd())
    def test_main(self, date: dt.datetime) -> None:
        assert isinstance(to_datetime(cast(Timestamp, date)), dt.datetime)
