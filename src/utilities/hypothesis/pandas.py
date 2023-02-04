import datetime as dt
from collections.abc import Hashable
from typing import Annotated, Any, Optional, cast

from beartype import beartype
from hypothesis.extra.pandas import indexes as _indexes
from hypothesis.strategies import (
    SearchStrategy,
    composite,
    dates,
    datetimes,
    integers,
)
from pandas import Index

from utilities.beartype.numpy import DTypeInt
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
def dates_pd(
    *,
    min_value: dt.date = TIMESTAMP_MIN_AS_DATE,
    max_value: dt.date = TIMESTAMP_MAX_AS_DATE,
) -> SearchStrategy[dt.date]:
    """Strategy for generating dates which can become Timestamps."""
    return dates(min_value=min_value, max_value=max_value)


@composite
@beartype
def datetimes_pd(
    _draw: Any,
    /,
    *,
    min_value: dt.datetime = TIMESTAMP_MIN_AS_DATETIME,
    max_value: dt.datetime = TIMESTAMP_MAX_AS_DATETIME,
) -> dt.datetime:
    """Strategy for generating datetimes which can become Timestamps."""
    draw = lift_draw(_draw)
    datetime = draw(
        datetimes(
            min_value=min_value.replace(tzinfo=None),
            max_value=max_value.replace(tzinfo=None),
        ),
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
        ),
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
) -> SearchStrategy[Annotated[Index, DTypeInt]]:
    """Strategy for generating integer Indexes."""
    return indexes(
        elements=int64s(),
        dtype=int,
        n=n,
        unique=unique,
        name=name,
        sort=sort,
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
        ),
    )
    return index.astype(string)
