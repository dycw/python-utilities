import datetime as dt

from hypothesis import given
from pandas import to_datetime

from dycw_utilities.hypothesis.pandas import dates_pd
from dycw_utilities.hypothesis.pandas import datetimes_pd


class TestDatesPd:
    @given(date=dates_pd())
    def test_main(self, date: dt.date) -> None:
        assert isinstance(to_datetime(date), dt.date)


class TestDatetimesPd:
    @given(date=datetimes_pd())
    def test_main(self, date: dt.datetime) -> None:
        assert isinstance(to_datetime(date), dt.datetime)
