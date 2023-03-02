import datetime as dt
from collections.abc import Hashable
from typing import Annotated, Any, Optional, cast

from beartype import beartype
from hypothesis import assume
from hypothesis.extra.pandas import indexes as _indexes
from hypothesis.strategies import SearchStrategy, composite, dates, datetimes, integers
from pandas import Index, Timedelta, Timestamp

from utilities.beartype.numpy import DTypeI
from utilities.beartype.pandas import DTypeString
from utilities.datetime import UTC
from utilities.hypothesis import lift_draw, text_ascii
from utilities.hypothesis.numpy import int64s
from utilities.hypothesis.typing import MaybeSearchStrategy
from utilities.pandas import (
    TIMESTAMP_MAX_AS_DATE,
    TIMESTAMP_MAX_AS_DATETIME,
    TIMESTAMP_MIN_AS_DATE,
    TIMESTAMP_MIN_AS_DATETIME,
    string,
)


@beartype
@composite
def dates_pd(
    _draw: Any,
    /,
    *,
    min_value: MaybeSearchStrategy[dt.date] = TIMESTAMP_MIN_AS_DATE,
    max_value: MaybeSearchStrategy[dt.date] = TIMESTAMP_MAX_AS_DATE,
) -> dt.date:
    """Strategy for generating dates which can become Timestamps."""
    draw = lift_draw(_draw)
    return draw(dates(min_value=draw(min_value), max_value=draw(max_value)))


@composite
@beartype
def datetimes_pd(
    _draw: Any,
    /,
    *,
    min_value: MaybeSearchStrategy[dt.datetime] = TIMESTAMP_MIN_AS_DATETIME,
    max_value: MaybeSearchStrategy[dt.datetime] = TIMESTAMP_MAX_AS_DATETIME,
) -> dt.datetime:
    """Strategy for generating datetimes which can become Timestamps."""
    draw = lift_draw(_draw)
    datetime = draw(
        datetimes(
            min_value=draw(min_value).replace(tzinfo=None),
            max_value=draw(max_value).replace(tzinfo=None),
        )
    )
    return datetime.replace(tzinfo=UTC)


_INDEX_LENGTHS = integers(0, 10)


@composite
@beartype
def indexes(
    _draw: Any,
    /,
    *,
    elements: Optional[SearchStrategy[Any]] = None,
    dtype: Any = None,
    n: MaybeSearchStrategy[int] = _INDEX_LENGTHS,
    unique: MaybeSearchStrategy[bool] = False,
    name: MaybeSearchStrategy[Hashable] = None,
    sort: MaybeSearchStrategy[bool] = False,
) -> Index:
    """Strategy for generating Indexes."""
    draw = lift_draw(_draw)
    n_ = draw(n)
    index = draw(
        _indexes(
            elements=elements,
            dtype=dtype,
            min_size=n_,
            max_size=n_,
            unique=draw(unique),
        )
    )
    index = cast(Index, index.rename(draw(name)))
    if draw(sort):
        return cast(Index, index.sort_values())
    return index


@beartype
def int_indexes(
    *,
    n: MaybeSearchStrategy[int] = _INDEX_LENGTHS,
    unique: MaybeSearchStrategy[bool] = False,
    name: MaybeSearchStrategy[Hashable] = None,
    sort: MaybeSearchStrategy[bool] = False,
) -> SearchStrategy[Annotated[Index, DTypeI]]:
    """Strategy for generating integer Indexes."""
    return indexes(
        elements=int64s(), dtype=int, n=n, unique=unique, name=name, sort=sort
    )


@composite
@beartype
def str_indexes(
    _draw: Any,
    /,
    *,
    n: MaybeSearchStrategy[int] = _INDEX_LENGTHS,
    unique: MaybeSearchStrategy[bool] = False,
    name: MaybeSearchStrategy[Hashable] = None,
    sort: MaybeSearchStrategy[bool] = False,
) -> Annotated[Index, DTypeString]:
    """Strategy for generating string Indexes."""
    draw = lift_draw(_draw)
    index = draw(
        indexes(
            elements=text_ascii(),
            dtype=object,
            n=n,
            unique=unique,
            name=name,
            sort=sort,
        )
    )
    return index.astype(string)


@composite
def timestamps(
    _draw: Any,
    /,
    *,
    min_value: MaybeSearchStrategy[dt.datetime] = TIMESTAMP_MIN_AS_DATETIME,
    max_value: MaybeSearchStrategy[dt.datetime] = TIMESTAMP_MAX_AS_DATETIME,
    allow_nanoseconds: MaybeSearchStrategy[bool] = False,
) -> Timestamp:
    """Strategy for generating Timestamps."""
    draw = lift_draw(_draw)
    min_value, max_value = map(draw, [min_value, max_value])
    datetime = draw(datetimes_pd(min_value=min_value, max_value=max_value))
    timestamp: Timestamp = Timestamp(datetime)
    if draw(allow_nanoseconds):
        nanoseconds = draw(integers(-999, 999))
        timedelta: Timedelta = Timedelta(nanoseconds=nanoseconds)
        timestamp += timedelta
        _ = assume(min_value <= timestamp.floor("us"))
        _ = assume(timestamp.ceil("us") <= max_value)
    return timestamp
