import datetime as dt

from hypothesis.strategies import SearchStrategy
from hypothesis.strategies import dates
from hypothesis.strategies import datetimes

from dycw_utilities.pandas import timestamp_max_as_date
from dycw_utilities.pandas import timestamp_max_as_datetime
from dycw_utilities.pandas import timestamp_min_as_date
from dycw_utilities.pandas import timestamp_min_as_datetime


def dates_pd(
    *,
    min_value: dt.date = timestamp_min_as_date(),
    max_value: dt.date = timestamp_max_as_date(),
) -> SearchStrategy[dt.date]:
    """Strategy for generating dates which can become Timestamps."""

    return dates(min_value=min_value, max_value=max_value)


def datetimes_pd(
    *,
    min_value: dt.datetime = timestamp_min_as_datetime(),
    max_value: dt.datetime = timestamp_max_as_datetime(),
) -> SearchStrategy[dt.datetime]:
    """Strategy for generating datetimes which can become Timestamps."""

    return datetimes(min_value=min_value, max_value=max_value)
