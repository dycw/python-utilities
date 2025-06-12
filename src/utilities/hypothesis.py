from __future__ import annotations

import builtins
import datetime as dt
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import timezone
from enum import Enum, auto
from functools import partial
from math import ceil, floor, inf, isclose, isfinite, nan
from os import environ
from pathlib import Path
from re import search
from string import ascii_letters, ascii_lowercase, ascii_uppercase, digits, printable
from subprocess import check_call
from typing import (
    TYPE_CHECKING,
    Any,
    Literal,
    TypeVar,
    assert_never,
    cast,
    overload,
    override,
)

import hypothesis.strategies
from hypothesis import HealthCheck, Phase, Verbosity, assume, settings
from hypothesis.errors import InvalidArgument
from hypothesis.strategies import (
    DataObject,
    DrawFn,
    SearchStrategy,
    booleans,
    characters,
    composite,
    dates,
    datetimes,
    floats,
    integers,
    just,
    lists,
    none,
    sampled_from,
    sets,
    text,
    timedeltas,
    uuids,
)
from hypothesis.utils.conventions import not_set
from whenever import Date, DateDelta, PlainDateTime, Time, TimeDelta, ZonedDateTime

from utilities.datetime import (
    DATETIME_MAX_NAIVE,
    DATETIME_MAX_UTC,
    DATETIME_MIN_NAIVE,
    DATETIME_MIN_UTC,
    DAY,
    MAX_DATE_TWO_DIGIT_YEAR,
    MAX_MONTH,
    MIN_DATE_TWO_DIGIT_YEAR,
    MIN_MONTH,
    Month,
    date_duration_to_int,
    date_duration_to_timedelta,
    date_to_month,
    datetime_duration_to_float,
    datetime_duration_to_timedelta,
    round_datetime,
)
from utilities.functions import ensure_int, ensure_str, max_nullable, min_nullable
from utilities.math import (
    MAX_FLOAT32,
    MAX_FLOAT64,
    MAX_INT32,
    MAX_INT64,
    MAX_UINT32,
    MAX_UINT64,
    MIN_FLOAT32,
    MIN_FLOAT64,
    MIN_INT32,
    MIN_INT64,
    MIN_UINT32,
    MIN_UINT64,
    is_zero,
)
from utilities.os import get_env_var
from utilities.pathlib import temp_cwd
from utilities.platform import IS_WINDOWS
from utilities.sentinel import Sentinel, sentinel
from utilities.tempfile import TEMP_DIR, TemporaryDirectory
from utilities.version import Version
from utilities.whenever2 import (
    DATE_DELTA_MAX,
    DATE_DELTA_MIN,
    DATE_DELTA_PARSABLE_MAX,
    DATE_DELTA_PARSABLE_MIN,
    DATE_MAX,
    DATE_MIN,
    PLAIN_DATE_TIME_MAX,
    PLAIN_DATE_TIME_MIN,
    TIME_DELTA_MAX,
    TIME_DELTA_MIN,
    TIME_MAX,
    TIME_MIN,
)
from utilities.zoneinfo import UTC, ensure_time_zone

if TYPE_CHECKING:
    from collections.abc import Collection, Hashable, Iterable, Iterator, Sequence
    from zoneinfo import ZoneInfo

    from hypothesis.database import ExampleDatabase
    from numpy.random import RandomState

    from utilities.numpy import NDArrayB, NDArrayF, NDArrayI, NDArrayO
    from utilities.types import Duration, MathRoundMode, Number, TimeZoneLike


_T = TypeVar("_T")
type MaybeSearchStrategy[_T] = _T | SearchStrategy[_T]
type Shape = int | tuple[int, ...]


##


@contextmanager
def assume_does_not_raise(
    *exceptions: type[Exception], match: str | None = None
) -> Iterator[None]:
    """Assume a set of exceptions are not raised.

    Optionally filter on the string representation of the exception.
    """
    try:
        yield
    except exceptions as caught:
        if match is None:
            _ = assume(condition=False)
        else:
            (msg,) = caught.args
            if search(match, ensure_str(msg)):
                _ = assume(condition=False)
            else:
                raise


##


@composite
def bool_arrays(
    draw: DrawFn,
    /,
    *,
    shape: MaybeSearchStrategy[Shape | None] = None,
    fill: MaybeSearchStrategy[Any] = None,
    unique: MaybeSearchStrategy[bool] = False,
) -> NDArrayB:
    """Strategy for generating arrays of booleans."""
    from hypothesis.extra.numpy import array_shapes, arrays

    strategy: SearchStrategy[NDArrayB] = arrays(
        bool,
        draw2(draw, shape, array_shapes()),
        elements=booleans(),
        fill=fill,
        unique=draw2(draw, unique),
    )
    return draw(strategy)


##


@composite
def date_deltas_whenever(
    draw: DrawFn,
    /,
    *,
    min_value: MaybeSearchStrategy[DateDelta | None] = None,
    max_value: MaybeSearchStrategy[DateDelta | None] = None,
    parsable: MaybeSearchStrategy[bool] = False,
) -> DateDelta:
    """Strategy for generating date deltas."""
    min_value_, max_value_ = [draw2(draw, v) for v in [min_value, max_value]]
    match min_value_:
        case None:
            min_value_ = DATE_DELTA_MIN
        case DateDelta():
            ...
        case _ as never:
            assert_never(never)
    match max_value_:
        case None:
            max_value_ = DATE_DELTA_MAX
        case DateDelta():
            ...
        case _ as never:
            assert_never(never)
    min_years, min_months, min_days = min_value_.in_years_months_days()
    assert min_years == 0
    assert min_months == 0
    max_years, max_months, max_days = max_value_.in_years_months_days()
    assert max_years == 0
    assert max_months == 0
    if draw2(draw, parsable):
        parsable_min_years, parsable_min_months, parsable_min_days = (
            DATE_DELTA_PARSABLE_MIN.in_years_months_days()
        )
        assert parsable_min_years == 0
        assert parsable_min_months == 0
        min_days = max(min_days, parsable_min_days)
        parsable_max_years, parsable_max_months, parsable_max_days = (
            DATE_DELTA_PARSABLE_MAX.in_years_months_days()
        )
        assert parsable_max_years == 0
        assert parsable_max_months == 0
        max_days = min(max_days, parsable_max_days)
    days = draw(integers(min_value=min_days, max_value=max_days))
    return DateDelta(days=days)


##


@composite
def date_durations(
    draw: DrawFn,
    /,
    *,
    min_int: MaybeSearchStrategy[int | None] = None,
    max_int: MaybeSearchStrategy[int | None] = None,
    min_timedelta: MaybeSearchStrategy[dt.timedelta | None] = None,
    max_timedelta: MaybeSearchStrategy[dt.timedelta | None] = None,
    two_way: bool = False,
) -> Duration:
    """Strategy for generating datetime durations."""
    min_int_, max_int_ = [draw2(draw, v) for v in [min_int, max_int]]
    min_timedelta_, max_timedelta_ = [
        draw2(draw, v) for v in [min_timedelta, max_timedelta]
    ]
    min_parts: Sequence[dt.timedelta | None] = [dt.timedelta.min, min_timedelta_]
    if min_int_ is not None:
        with assume_does_not_raise(OverflowError):
            min_parts.append(date_duration_to_timedelta(min_int_))
    if two_way:
        from utilities.whenever import MIN_SERIALIZABLE_TIMEDELTA

        min_parts.append(MIN_SERIALIZABLE_TIMEDELTA)
    min_timedelta_use = max_nullable(min_parts)
    max_parts: Sequence[dt.timedelta | None] = [dt.timedelta.max, max_timedelta_]
    if max_int_ is not None:
        with assume_does_not_raise(OverflowError):
            max_parts.append(date_duration_to_timedelta(max_int_))
    if two_way:
        from utilities.whenever import MAX_SERIALIZABLE_TIMEDELTA

        max_parts.append(MAX_SERIALIZABLE_TIMEDELTA)
    max_timedelta_use = min_nullable(max_parts)
    _ = assume(min_timedelta_use <= max_timedelta_use)
    st_timedeltas = (
        timedeltas(min_value=min_timedelta_use, max_value=max_timedelta_use)
        .map(_round_timedelta)
        .filter(
            partial(
                _is_between_timedelta, min_=min_timedelta_use, max_=max_timedelta_use
            )
        )
    )
    st_integers = st_timedeltas.map(date_duration_to_int)
    st_floats = st_integers.map(float)
    return draw(st_integers | st_floats | st_timedeltas)


def _round_timedelta(timedelta: dt.timedelta, /) -> dt.timedelta:
    return dt.timedelta(days=timedelta.days)


def _is_between_timedelta(
    timedelta: dt.timedelta, /, *, min_: dt.timedelta, max_: dt.timedelta
) -> bool:
    return min_ <= timedelta <= max_


##


@composite
def dates_two_digit_year(
    draw: DrawFn,
    /,
    *,
    min_value: MaybeSearchStrategy[dt.date] = MIN_DATE_TWO_DIGIT_YEAR,
    max_value: MaybeSearchStrategy[dt.date] = MAX_DATE_TWO_DIGIT_YEAR,
) -> dt.date:
    """Strategy for generating dates with valid 2 digit years."""
    min_value_, max_value_ = [draw2(draw, v) for v in [min_value, max_value]]
    min_value_ = max(min_value_, MIN_DATE_TWO_DIGIT_YEAR)
    max_value_ = min(max_value_, MAX_DATE_TWO_DIGIT_YEAR)
    return draw(dates(min_value=min_value_, max_value=max_value_))


##


@composite
def dates_whenever(
    draw: DrawFn,
    /,
    *,
    min_value: MaybeSearchStrategy[Date | None] = None,
    max_value: MaybeSearchStrategy[Date | None] = None,
) -> Date:
    """Strategy for generating dates."""
    min_value_, max_value_ = [draw2(draw, v) for v in [min_value, max_value]]
    match min_value_:
        case None:
            min_value_ = DATE_MIN
        case Date():
            ...
        case _ as never:
            assert_never(never)
    match max_value_:
        case None:
            max_value_ = DATE_MAX
        case Date():
            ...
        case _ as never:
            assert_never(never)
    py_date = draw(
        dates(min_value=min_value_.py_date(), max_value=max_value_.py_date())
    )
    return Date.from_py_date(py_date)


##


@composite
def datetime_durations(
    draw: DrawFn,
    /,
    *,
    min_number: MaybeSearchStrategy[Number | None] = None,
    max_number: MaybeSearchStrategy[Number | None] = None,
    min_timedelta: MaybeSearchStrategy[dt.timedelta | None] = None,
    max_timedelta: MaybeSearchStrategy[dt.timedelta | None] = None,
    two_way: bool = False,
) -> Duration:
    """Strategy for generating datetime durations."""
    min_number_, max_number_ = [draw2(draw, v) for v in [min_number, max_number]]
    min_timedelta_, max_timedelta_ = [
        draw2(draw, v) for v in [min_timedelta, max_timedelta]
    ]
    min_parts = [min_timedelta_, dt.timedelta.min]
    if min_number_ is not None:
        with assume_does_not_raise(OverflowError):
            min_parts.append(datetime_duration_to_timedelta(min_number_))
    if two_way:
        from utilities.whenever import MIN_SERIALIZABLE_TIMEDELTA

        min_parts.append(MIN_SERIALIZABLE_TIMEDELTA)
    min_timedelta_use = max_nullable(min_parts)
    max_parts = [max_timedelta_, dt.timedelta.max]
    if max_number_ is not None:
        with assume_does_not_raise(OverflowError):
            max_parts.append(datetime_duration_to_timedelta(max_number_))
    if two_way:
        from utilities.whenever import MAX_SERIALIZABLE_TIMEDELTA

        max_parts.append(MAX_SERIALIZABLE_TIMEDELTA)
    max_timedelta_use = min_nullable(max_parts)
    _ = assume(min_timedelta_use <= max_timedelta_use)
    min_float_use, max_float_use = map(
        datetime_duration_to_float, [min_timedelta_use, max_timedelta_use]
    )
    _ = assume(min_float_use <= max_float_use)
    st_numbers = numbers(min_value=min_float_use, max_value=max_float_use)
    st_timedeltas = timedeltas(min_value=min_timedelta_use, max_value=max_timedelta_use)
    return draw(st_numbers | st_timedeltas)


##


@overload
def draw2(
    data_or_draw: DataObject | DrawFn,
    maybe_strategy: MaybeSearchStrategy[_T],
    /,
    *,
    sentinel: bool = False,
) -> _T: ...
@overload
def draw2(
    data_or_draw: DataObject | DrawFn,
    maybe_strategy: MaybeSearchStrategy[_T | None | Sentinel],
    default: SearchStrategy[_T | None],
    /,
    *,
    sentinel: Literal[True],
) -> _T | None: ...
@overload
def draw2(
    data_or_draw: DataObject | DrawFn,
    maybe_strategy: MaybeSearchStrategy[_T | None],
    default: SearchStrategy[_T],
    /,
    *,
    sentinel: Literal[False] = False,
) -> _T: ...
@overload
def draw2(
    data_or_draw: DataObject | DrawFn,
    maybe_strategy: MaybeSearchStrategy[_T | None | Sentinel],
    default: SearchStrategy[_T] | None = None,
    /,
    *,
    sentinel: bool = False,
) -> _T | None: ...
def draw2(
    data_or_draw: DataObject | DrawFn,
    maybe_strategy: MaybeSearchStrategy[_T | None | Sentinel],
    default: SearchStrategy[_T | None] | None = None,
    /,
    *,
    sentinel: bool = False,
) -> _T | None:
    """Draw an element from a strategy, unless you require it to be non-nullable."""
    draw = data_or_draw.draw if isinstance(data_or_draw, DataObject) else data_or_draw
    if isinstance(maybe_strategy, SearchStrategy):
        value = draw(maybe_strategy)
    else:
        value = maybe_strategy
    match value, default, sentinel:
        case (None, None, _):
            return value
        case None, SearchStrategy(), True:
            return value
        case None, SearchStrategy(), False:
            value2 = draw(default)
            if isinstance(value2, Sentinel):
                raise _Draw2DefaultGeneratedSentinelError
            return value2
        case Sentinel(), None, _:
            raise _Draw2InputResolvedToSentinelError
        case Sentinel(), SearchStrategy(), True:
            value2 = draw(default)
            if isinstance(value2, Sentinel):
                raise _Draw2DefaultGeneratedSentinelError
            return value2
        case Sentinel(), SearchStrategy(), False:
            raise _Draw2InputResolvedToSentinelError
        case _, _, _:
            return value
        case _ as never:
            assert_never(never)


@dataclass(kw_only=True, slots=True)
class Draw2Error(Exception): ...


@dataclass(kw_only=True, slots=True)
class _Draw2InputResolvedToSentinelError(Draw2Error):
    @override
    def __str__(self) -> str:
        return "The input resolved to the sentinel value; a default strategy is needed"


@dataclass(kw_only=True, slots=True)
class _Draw2DefaultGeneratedSentinelError(Draw2Error):
    @override
    def __str__(self) -> str:
        return "The default search strategy generated the sentinel value"


##


@composite
def float32s(
    draw: DrawFn,
    /,
    *,
    min_value: MaybeSearchStrategy[float] = MIN_FLOAT32,
    max_value: MaybeSearchStrategy[float] = MAX_FLOAT32,
) -> float:
    """Strategy for generating float32s."""
    min_value_, max_value_ = [draw2(draw, v) for v in [min_value, max_value]]
    min_value_ = max(min_value_, MIN_FLOAT32)
    max_value_ = min(max_value_, MAX_FLOAT32)
    if is_zero(min_value_) and is_zero(max_value_):
        min_value_ = max_value_ = 0.0
    return draw(floats(min_value_, max_value_, width=32))


@composite
def float64s(
    draw: DrawFn,
    /,
    *,
    min_value: MaybeSearchStrategy[float] = MIN_FLOAT64,
    max_value: MaybeSearchStrategy[float] = MAX_FLOAT64,
) -> float:
    """Strategy for generating float64s."""
    min_value_, max_value_ = [draw2(draw, v) for v in [min_value, max_value]]
    min_value_ = max(min_value_, MIN_FLOAT64)
    max_value_ = min(max_value_, MAX_FLOAT64)
    if is_zero(min_value_) and is_zero(max_value_):
        min_value_ = max_value_ = 0.0
    return draw(floats(min_value_, max_value_, width=64))


##


@composite
def float_arrays(
    draw: DrawFn,
    /,
    *,
    shape: MaybeSearchStrategy[Shape | None] = None,
    min_value: MaybeSearchStrategy[float | None] = None,
    max_value: MaybeSearchStrategy[float | None] = None,
    allow_nan: MaybeSearchStrategy[bool] = False,
    allow_inf: MaybeSearchStrategy[bool] = False,
    allow_pos_inf: MaybeSearchStrategy[bool] = False,
    allow_neg_inf: MaybeSearchStrategy[bool] = False,
    integral: MaybeSearchStrategy[bool] = False,
    fill: MaybeSearchStrategy[Any] = None,
    unique: MaybeSearchStrategy[bool] = False,
) -> NDArrayF:
    """Strategy for generating arrays of floats."""
    from hypothesis.extra.numpy import array_shapes, arrays

    elements = floats_extra(
        min_value=min_value,
        max_value=max_value,
        allow_nan=allow_nan,
        allow_inf=allow_inf,
        allow_pos_inf=allow_pos_inf,
        allow_neg_inf=allow_neg_inf,
        integral=integral,
    )
    strategy: SearchStrategy[NDArrayF] = arrays(
        float,
        draw2(draw, shape, array_shapes()),
        elements=elements,
        fill=fill,
        unique=draw2(draw, unique),
    )
    return draw(strategy)


##


@composite
def floats_extra(
    draw: DrawFn,
    /,
    *,
    min_value: MaybeSearchStrategy[float | None] = None,
    max_value: MaybeSearchStrategy[float | None] = None,
    allow_nan: MaybeSearchStrategy[bool] = False,
    allow_inf: MaybeSearchStrategy[bool] = False,
    allow_pos_inf: MaybeSearchStrategy[bool] = False,
    allow_neg_inf: MaybeSearchStrategy[bool] = False,
    integral: MaybeSearchStrategy[bool] = False,
) -> float:
    """Strategy for generating floats, with extra special values."""
    min_value_, max_value_ = [draw2(draw, v) for v in [min_value, max_value]]
    elements = floats(
        min_value=min_value_,
        max_value=max_value_,
        allow_nan=False,
        allow_infinity=False,
    )
    if draw2(draw, allow_nan):
        elements |= just(nan)
    if draw2(draw, allow_inf):
        elements |= sampled_from([inf, -inf])
    if draw2(draw, allow_pos_inf):
        elements |= just(inf)
    if draw2(draw, allow_neg_inf):
        elements |= just(-inf)
    element = draw2(draw, elements)
    if isfinite(element) and draw2(draw, integral):
        candidates = [floor(element), ceil(element)]
        if min_value_ is not None:
            candidates = [c for c in candidates if c >= min_value_]
        if max_value_ is not None:
            candidates = [c for c in candidates if c <= max_value_]
        _ = assume(len(candidates) >= 1)
        element = draw2(draw, sampled_from(candidates))
        return float(element)
    return element


##


@composite
def git_repos(draw: DrawFn, /) -> Path:
    path = draw(temp_paths())
    with temp_cwd(path):
        _ = check_call(["git", "init", "-b", "master"])
        _ = check_call(["git", "config", "user.name", "User"])
        _ = check_call(["git", "config", "user.email", "a@z.com"])
        file = Path(path, "file")
        file.touch()
        file_str = str(file)
        _ = check_call(["git", "add", file_str])
        _ = check_call(["git", "commit", "-m", "add"])
        _ = check_call(["git", "rm", file_str])
        _ = check_call(["git", "commit", "-m", "rm"])
    return path


##


def hashables() -> SearchStrategy[Hashable]:
    """Strategy for generating hashable elements."""
    return booleans() | integers() | none() | text_ascii()


##


@composite
def int_arrays(
    draw: DrawFn,
    /,
    *,
    shape: MaybeSearchStrategy[Shape | None] = None,
    min_value: MaybeSearchStrategy[int] = MIN_INT64,
    max_value: MaybeSearchStrategy[int] = MAX_INT64,
    fill: MaybeSearchStrategy[Any] = None,
    unique: MaybeSearchStrategy[bool] = False,
) -> NDArrayI:
    """Strategy for generating arrays of ints."""
    from hypothesis.extra.numpy import array_shapes, arrays
    from numpy import int64

    elements = int64s(min_value=min_value, max_value=max_value)
    strategy: SearchStrategy[NDArrayI] = arrays(
        int64,
        draw2(draw, shape, array_shapes()),
        elements=elements,
        fill=fill,
        unique=draw2(draw, unique),
    )
    return draw(strategy)


##


@composite
def int32s(
    draw: DrawFn,
    /,
    *,
    min_value: MaybeSearchStrategy[int] = MIN_INT32,
    max_value: MaybeSearchStrategy[int] = MAX_INT32,
) -> int:
    """Strategy for generating int32s."""
    min_value_, max_value_ = [draw2(draw, v) for v in [min_value, max_value]]
    min_value_ = max(min_value_, MIN_INT32)
    max_value_ = min(max_value_, MAX_INT32)
    return draw(integers(min_value_, max_value_))


@composite
def int64s(
    draw: DrawFn,
    /,
    *,
    min_value: MaybeSearchStrategy[int] = MIN_INT64,
    max_value: MaybeSearchStrategy[int] = MAX_INT64,
) -> int:
    """Strategy for generating int64s."""
    min_value_, max_value_ = [draw2(draw, v) for v in [min_value, max_value]]
    min_value_ = max(min_value_, MIN_INT64)
    max_value_ = min(max_value_, MAX_INT64)
    return draw(integers(min_value_, max_value_))


##


@composite
def lists_fixed_length(
    draw: DrawFn,
    strategy: SearchStrategy[_T],
    size: MaybeSearchStrategy[int],
    /,
    *,
    unique: MaybeSearchStrategy[bool] = False,
    sorted: MaybeSearchStrategy[bool] = False,  # noqa: A002
) -> list[_T]:
    """Strategy for generating lists of a fixed length."""
    size_ = draw2(draw, size)
    elements = draw(
        lists(strategy, min_size=size_, max_size=size_, unique=draw2(draw, unique))
    )
    if draw2(draw, sorted):
        return builtins.sorted(cast("Iterable[Any]", elements))
    return elements


##


@composite
def min_and_max_datetimes(
    draw: DrawFn,
    /,
    *,
    min_value: MaybeSearchStrategy[dt.datetime | None] = None,
    max_value: MaybeSearchStrategy[dt.datetime | None] = None,
    time_zone: MaybeSearchStrategy[ZoneInfo | timezone] = UTC,
    round_: MathRoundMode | None = None,
    timedelta: dt.timedelta | None = None,
    rel_tol: float | None = None,
    abs_tol: float | None = None,
    valid: bool = False,
) -> tuple[dt.datetime, dt.datetime]:
    """Strategy for generating min/max datetimes."""
    match min_value, max_value:
        case None, None:
            return draw(
                pairs(
                    zoned_datetimes(
                        time_zone=time_zone,
                        round_=round_,
                        timedelta=timedelta,
                        rel_tol=rel_tol,
                        abs_tol=abs_tol,
                        valid=valid,
                    ),
                    sorted=True,
                )
            )
        case None, dt.datetime():
            min_value_ = draw(
                zoned_datetimes(
                    max_value=max_value,
                    time_zone=time_zone,
                    round_=round_,
                    timedelta=timedelta,
                    rel_tol=rel_tol,
                    abs_tol=abs_tol,
                    valid=valid,
                )
            )
            return min_value_, max_value
        case dt.datetime(), None:
            max_value_ = draw(
                zoned_datetimes(
                    min_value=min_value,
                    time_zone=time_zone,
                    round_=round_,
                    timedelta=timedelta,
                    rel_tol=rel_tol,
                    abs_tol=abs_tol,
                    valid=valid,
                )
            )
            return min_value, max_value_
        case dt.datetime(), dt.datetime():
            _ = assume(min_value <= max_value)
            return min_value, max_value
        case _, _:
            strategy = zoned_datetimes(
                time_zone=time_zone,
                round_=round_,
                timedelta=timedelta,
                rel_tol=rel_tol,
                abs_tol=abs_tol,
                valid=valid,
            )
            min_value_ = draw2(draw, min_value, strategy)
            max_value_ = draw2(draw, max_value, strategy)
            _ = assume(min_value_ <= max_value_)
            return min_value_, max_value_
        case _ as never:
            assert_never(never)


##


@composite
def months(
    draw: DrawFn,
    /,
    *,
    min_value: MaybeSearchStrategy[Month] = MIN_MONTH,
    max_value: MaybeSearchStrategy[Month] = MAX_MONTH,
) -> Month:
    """Strategy for generating datetimes with the UTC timezone."""
    min_value_, max_value_ = [draw2(draw, v).to_date() for v in [min_value, max_value]]
    date = draw(dates(min_value=min_value_, max_value=max_value_))
    return date_to_month(date)


##


@composite
def namespace_mixins(draw: DrawFn, /) -> type:
    """Strategy for generating task namespace mixins."""
    path = draw(temp_paths())

    class NamespaceMixin:
        task_namespace = path.name

    return NamespaceMixin


##


@composite
def numbers(
    draw: DrawFn,
    /,
    *,
    min_value: MaybeSearchStrategy[Number | None] = None,
    max_value: MaybeSearchStrategy[Number | None] = None,
) -> int | float:
    """Strategy for generating numbers."""
    min_value_, max_value_ = [draw2(draw, v) for v in [min_value, max_value]]
    if (min_value_ is None) or isinstance(min_value_, int):
        min_int = min_value_
    else:
        min_int = ceil(min_value_)
    if (max_value_ is None) or isinstance(max_value_, int):
        max_int = max_value_
    else:
        max_int = floor(max_value_)
    if (min_int is not None) and (max_int is not None):
        _ = assume(min_int <= max_int)
    st_integers = integers(min_int, max_int)
    if (
        (min_value_ is not None)
        and isclose(min_value_, 0.0)
        and (max_value_ is not None)
        and isclose(max_value_, 0.0)
    ):
        min_value_ = max_value_ = 0.0
    st_floats = floats(
        min_value=min_value_,
        max_value=max_value_,
        allow_nan=False,
        allow_infinity=False,
    )
    return draw(st_integers | st_floats)


##


def pairs(
    strategy: SearchStrategy[_T],
    /,
    *,
    unique: MaybeSearchStrategy[bool] = False,
    sorted: MaybeSearchStrategy[bool] = False,  # noqa: A002
) -> SearchStrategy[tuple[_T, _T]]:
    """Strategy for generating pairs of elements."""
    return lists_fixed_length(strategy, 2, unique=unique, sorted=sorted).map(_pairs_map)


def _pairs_map(elements: list[_T], /) -> tuple[_T, _T]:
    first, second = elements
    return first, second


##


def paths() -> SearchStrategy[Path]:
    """Strategy for generating `Path`s."""
    reserved = {"AUX", "NUL"}
    strategy = text_ascii(min_size=1, max_size=10).filter(lambda x: x not in reserved)
    return lists(strategy, max_size=10).map(lambda parts: Path(*parts))


##


@composite
def plain_datetimes(
    draw: DrawFn,
    /,
    *,
    min_value: MaybeSearchStrategy[dt.datetime] = DATETIME_MIN_NAIVE,
    max_value: MaybeSearchStrategy[dt.datetime] = DATETIME_MAX_NAIVE,
    round_: MathRoundMode | None = None,
    timedelta: dt.timedelta | None = None,
    rel_tol: float | None = None,
    abs_tol: float | None = None,
) -> dt.datetime:
    """Strategy for generating plain datetimes."""
    min_value_, max_value_ = [draw2(draw, v) for v in [min_value, max_value]]
    datetime = draw(datetimes(min_value=min_value_, max_value=max_value_))
    if round_ is not None:
        if timedelta is None:
            raise PlainDateTimesError(round_=round_)
        datetime = round_datetime(
            datetime, timedelta, mode=round_, rel_tol=rel_tol, abs_tol=abs_tol
        )
        _ = assume(min_value_ <= datetime <= max_value_)
    return datetime


@dataclass(kw_only=True, slots=True)
class PlainDateTimesError(Exception):
    round_: MathRoundMode

    @override
    def __str__(self) -> str:
        return "Rounding requires a timedelta; got None"


##


@composite
def plain_datetimes_whenever(
    draw: DrawFn,
    /,
    *,
    min_value: MaybeSearchStrategy[PlainDateTime | None] = None,
    max_value: MaybeSearchStrategy[PlainDateTime | None] = None,
) -> PlainDateTime:
    """Strategy for generating plain datetimes."""
    min_value_, max_value_ = [draw2(draw, v) for v in [min_value, max_value]]
    match min_value_:
        case None:
            min_value_ = PLAIN_DATE_TIME_MIN
        case PlainDateTime():
            ...
        case _ as never:
            assert_never(never)
    match max_value_:
        case None:
            max_value_ = PLAIN_DATE_TIME_MAX
        case PlainDateTime():
            ...
        case _ as never:
            assert_never(never)
    py_datetime = draw(
        datetimes(
            min_value=min_value_.py_datetime(), max_value=max_value_.py_datetime()
        )
    )
    return PlainDateTime.from_py_datetime(py_datetime)


##


@composite
def random_states(
    draw: DrawFn, /, *, seed: MaybeSearchStrategy[int | None] = None
) -> RandomState:
    """Strategy for generating `numpy` random states."""
    from numpy.random import RandomState

    seed_ = draw2(draw, seed, integers(0, MAX_UINT32))
    return RandomState(seed=seed_)


##


def sentinels() -> SearchStrategy[Sentinel]:
    """Strategy for generating sentinels."""
    return just(sentinel)


##


@composite
def sets_fixed_length(
    draw: DrawFn, strategy: SearchStrategy[_T], size: MaybeSearchStrategy[int], /
) -> set[_T]:
    """Strategy for generating lists of a fixed length."""
    size_ = draw2(draw, size)
    return draw(sets(strategy, min_size=size_, max_size=size_))


##


def setup_hypothesis_profiles(
    *, suppress_health_check: Iterable[HealthCheck] = ()
) -> None:
    """Set up the hypothesis profiles."""

    class Profile(Enum):
        dev = auto()
        default = auto()
        ci = auto()
        debug = auto()

        @property
        def max_examples(self) -> int:
            match self:
                case Profile.dev | Profile.debug:
                    return 10
                case Profile.default:
                    return 100
                case Profile.ci:
                    return 1000
                case _ as never:
                    assert_never(never)

        @property
        def verbosity(self) -> Verbosity:
            match self:
                case Profile.dev | Profile.debug | Profile.default:
                    return Verbosity.quiet
                case Profile.ci:
                    return Verbosity.verbose
                case _ as never:
                    assert_never(never)

    phases = {Phase.explicit, Phase.reuse, Phase.generate, Phase.target}
    if "HYPOTHESIS_NO_SHRINK" not in environ:
        phases.add(Phase.shrink)
    for profile in Profile:
        try:
            max_examples = int(environ["HYPOTHESIS_MAX_EXAMPLES"])
        except KeyError:
            max_examples = profile.max_examples
        settings.register_profile(
            profile.name,
            max_examples=max_examples,
            phases=phases,
            report_multiple_bugs=False,
            deadline=None,
            print_blob=True,
            suppress_health_check=suppress_health_check,
            verbosity=profile.verbosity,
        )
    profile = get_env_var("HYPOTHESIS_PROFILE", default=Profile.default.name)
    settings.load_profile(profile)


##


def settings_with_reduced_examples(
    frac: float = 0.1,
    /,
    *,
    derandomize: bool = not_set,  # pyright: ignore[reportArgumentType]
    database: ExampleDatabase | None = not_set,  # pyright: ignore[reportArgumentType]
    verbosity: Verbosity = not_set,  # pyright: ignore[reportArgumentType]
    phases: Collection[Phase] = not_set,  # pyright: ignore[reportArgumentType]
    stateful_step_count: int = not_set,  # pyright: ignore[reportArgumentType]
    report_multiple_bugs: bool = not_set,  # pyright: ignore[reportArgumentType]
    suppress_health_check: Collection[HealthCheck] = not_set,  # pyright: ignore[reportArgumentType]
    deadline: float | dt.timedelta | None = not_set,  # pyright: ignore[reportArgumentType]
    print_blob: bool = not_set,  # pyright: ignore[reportArgumentType]
    backend: str = not_set,  # pyright: ignore[reportArgumentType]
) -> settings:
    """Set a test to fewer max examples."""
    curr = settings()
    max_examples = max(round(frac * ensure_int(curr.max_examples)), 1)
    return settings(
        max_examples=max_examples,
        derandomize=derandomize,
        database=database,
        verbosity=verbosity,
        phases=phases,
        stateful_step_count=stateful_step_count,
        report_multiple_bugs=report_multiple_bugs,
        suppress_health_check=suppress_health_check,
        deadline=deadline,
        print_blob=print_blob,
        backend=backend,
    )


##


@composite
def slices(
    draw: DrawFn,
    iter_len: MaybeSearchStrategy[int],
    /,
    *,
    slice_len: MaybeSearchStrategy[int | None] = None,
) -> slice:
    """Strategy for generating continuous slices from an iterable."""
    iter_len_ = draw2(draw, iter_len)
    slice_len_ = draw2(draw, slice_len, integers(0, iter_len_))
    if not 0 <= slice_len_ <= iter_len_:
        msg = f"Slice length {slice_len_} exceeds iterable length {iter_len_}"
        raise InvalidArgument(msg)
    start = draw(integers(0, iter_len_ - slice_len_))
    stop = start + slice_len_
    return slice(start, stop)


##


@composite
def str_arrays(
    draw: DrawFn,
    /,
    *,
    shape: MaybeSearchStrategy[Shape | None] = None,
    min_size: MaybeSearchStrategy[int] = 0,
    max_size: MaybeSearchStrategy[int | None] = None,
    allow_none: MaybeSearchStrategy[bool] = False,
    fill: MaybeSearchStrategy[Any] = None,
    unique: MaybeSearchStrategy[bool] = False,
) -> NDArrayO:
    """Strategy for generating arrays of strings."""
    from hypothesis.extra.numpy import array_shapes, arrays

    elements = text_ascii(min_size=min_size, max_size=max_size)
    if draw2(draw, allow_none):
        elements |= none()
    strategy: SearchStrategy[NDArrayO] = arrays(
        object,
        draw2(draw, shape, array_shapes()),
        elements=elements,
        fill=fill,
        unique=draw2(draw, unique),
    )
    return draw(strategy)


##


_TEMP_DIR_HYPOTHESIS = Path(TEMP_DIR, "hypothesis")


@composite
def temp_dirs(draw: DrawFn, /) -> TemporaryDirectory:
    """Search strategy for temporary directories."""
    _TEMP_DIR_HYPOTHESIS.mkdir(exist_ok=True)
    uuid = draw(uuids())
    return TemporaryDirectory(
        prefix=f"{uuid}__", dir=_TEMP_DIR_HYPOTHESIS, ignore_cleanup_errors=IS_WINDOWS
    )


##


@composite
def temp_paths(draw: DrawFn, /) -> Path:
    """Search strategy for paths to temporary directories."""
    temp_dir = draw(temp_dirs())
    root = temp_dir.path
    cls = type(root)

    class SubPath(cls):
        _temp_dir = temp_dir

    return SubPath(root)


##


@composite
def text_ascii(
    draw: DrawFn,
    /,
    *,
    min_size: MaybeSearchStrategy[int] = 0,
    max_size: MaybeSearchStrategy[int | None] = None,
) -> str:
    """Strategy for generating ASCII text."""
    alphabet = characters(whitelist_categories=[], whitelist_characters=ascii_letters)
    return draw(
        text(alphabet, min_size=draw2(draw, min_size), max_size=draw2(draw, max_size))
    )


@composite
def text_ascii_lower(
    draw: DrawFn,
    /,
    *,
    min_size: MaybeSearchStrategy[int] = 0,
    max_size: MaybeSearchStrategy[int | None] = None,
) -> str:
    """Strategy for generating ASCII lower-case text."""
    alphabet = characters(whitelist_categories=[], whitelist_characters=ascii_lowercase)
    return draw(
        text(alphabet, min_size=draw2(draw, min_size), max_size=draw2(draw, max_size))
    )


@composite
def text_ascii_upper(
    draw: DrawFn,
    /,
    *,
    min_size: MaybeSearchStrategy[int] = 0,
    max_size: MaybeSearchStrategy[int | None] = None,
) -> str:
    """Strategy for generating ASCII upper-case text."""
    alphabet = characters(whitelist_categories=[], whitelist_characters=ascii_uppercase)
    return draw(
        text(alphabet, min_size=draw2(draw, min_size), max_size=draw2(draw, max_size))
    )


@composite
def text_clean(
    draw: DrawFn,
    /,
    *,
    min_size: MaybeSearchStrategy[int] = 0,
    max_size: MaybeSearchStrategy[int | None] = None,
) -> str:
    """Strategy for generating clean text."""
    alphabet = characters(blacklist_categories=["Z", "C"])
    return draw(
        text(alphabet, min_size=draw2(draw, min_size), max_size=draw2(draw, max_size))
    )


@composite
def text_digits(
    draw: DrawFn,
    /,
    *,
    min_size: MaybeSearchStrategy[int] = 0,
    max_size: MaybeSearchStrategy[int | None] = None,
) -> str:
    """Strategy for generating ASCII text."""
    alphabet = characters(whitelist_categories=[], whitelist_characters=digits)
    return draw(
        text(alphabet, min_size=draw2(draw, min_size), max_size=draw2(draw, max_size))
    )


@composite
def text_printable(
    draw: DrawFn,
    /,
    *,
    min_size: MaybeSearchStrategy[int] = 0,
    max_size: MaybeSearchStrategy[int | None] = None,
) -> str:
    """Strategy for generating printable text."""
    alphabet = characters(whitelist_categories=[], whitelist_characters=printable)
    return draw(
        text(alphabet, min_size=draw2(draw, min_size), max_size=draw2(draw, max_size))
    )


##


@composite
def time_deltas_whenever(
    draw: DrawFn,
    /,
    *,
    min_value: MaybeSearchStrategy[TimeDelta | None] = None,
    max_value: MaybeSearchStrategy[TimeDelta | None] = None,
) -> TimeDelta:
    """Strategy for generating time deltas."""
    min_value_, max_value_ = [draw2(draw, v) for v in [min_value, max_value]]
    match min_value_:
        case None:
            min_value_ = TIME_DELTA_MIN
        case TimeDelta():
            ...
        case _ as never:
            assert_never(never)
    match max_value_:
        case None:
            max_value_ = TIME_DELTA_MAX
        case TimeDelta():
            ...
        case _ as never:
            assert_never(never)
    py_time = draw(
        hypothesis.strategies.timedeltas(
            min_value=min_value_.py_timedelta(), max_value=max_value_.py_timedelta()
        )
    )
    return TimeDelta.from_py_timedelta(py_time)


##


@composite
def timedeltas_2w(
    draw: DrawFn,
    /,
    *,
    min_value: MaybeSearchStrategy[dt.timedelta] = dt.timedelta.min,
    max_value: MaybeSearchStrategy[dt.timedelta] = dt.timedelta.max,
) -> dt.timedelta:
    """Strategy for generating timedeltas which can be se/deserialized."""
    from utilities.whenever import (
        MAX_SERIALIZABLE_TIMEDELTA,
        MIN_SERIALIZABLE_TIMEDELTA,
    )

    min_value_, max_value_ = [draw2(draw, v) for v in [min_value, max_value]]
    return draw(
        timedeltas(
            min_value=max(min_value_, MIN_SERIALIZABLE_TIMEDELTA),
            max_value=min(max_value_, MAX_SERIALIZABLE_TIMEDELTA),
        )
    )


##


@composite
def times_whenever(
    draw: DrawFn,
    /,
    *,
    min_value: MaybeSearchStrategy[Time | None] = None,
    max_value: MaybeSearchStrategy[Time | None] = None,
) -> Time:
    """Strategy for generating times."""
    min_value_, max_value_ = [draw2(draw, v) for v in [min_value, max_value]]
    match min_value_:
        case None:
            min_value_ = TIME_MIN
        case Time():
            ...
        case _ as never:
            assert_never(never)
    match max_value_:
        case None:
            max_value_ = TIME_MAX
        case Time():
            ...
        case _ as never:
            assert_never(never)
    py_time = draw(
        hypothesis.strategies.times(
            min_value=min_value_.py_time(), max_value=max_value_.py_time()
        )
    )
    return Time.from_py_time(py_time)


##


def triples(
    strategy: SearchStrategy[_T],
    /,
    *,
    unique: MaybeSearchStrategy[bool] = False,
    sorted: MaybeSearchStrategy[bool] = False,  # noqa: A002
) -> SearchStrategy[tuple[_T, _T, _T]]:
    """Strategy for generating triples of elements."""
    return lists_fixed_length(strategy, 3, unique=unique, sorted=sorted).map(
        _triples_map
    )


def _triples_map(elements: list[_T], /) -> tuple[_T, _T, _T]:
    first, second, third = elements
    return first, second, third


##


@composite
def uint32s(
    draw: DrawFn,
    /,
    *,
    min_value: MaybeSearchStrategy[int] = MIN_UINT32,
    max_value: MaybeSearchStrategy[int] = MAX_UINT32,
) -> int:
    """Strategy for generating uint32s."""
    min_value_, max_value_ = [draw2(draw, v) for v in [min_value, max_value]]
    min_value_ = max(min_value_, MIN_UINT32)
    max_value_ = min(max_value_, MAX_UINT32)
    return draw(integers(min_value=min_value_, max_value=max_value_))


@composite
def uint64s(
    draw: DrawFn,
    /,
    *,
    min_value: MaybeSearchStrategy[int] = MIN_UINT64,
    max_value: MaybeSearchStrategy[int] = MAX_UINT64,
) -> int:
    """Strategy for generating uint64s."""
    min_value_, max_value_ = [draw2(draw, v) for v in [min_value, max_value]]
    min_value_ = max(min_value_, MIN_UINT64)
    max_value_ = min(max_value_, MAX_UINT64)
    return draw(integers(min_value=min_value_, max_value=max_value_))


##


@composite
def versions(draw: DrawFn, /, *, suffix: MaybeSearchStrategy[bool] = False) -> Version:
    """Strategy for generating versions."""
    major, minor, patch = draw(triples(integers(min_value=0)))
    _ = assume((major >= 1) or (minor >= 1) or (patch >= 1))
    suffix_use = draw(text_ascii(min_size=1)) if draw2(draw, suffix) else None
    return Version(major=major, minor=minor, patch=patch, suffix=suffix_use)


##


@composite
def zoned_datetimes(
    draw: DrawFn,
    /,
    *,
    min_value: MaybeSearchStrategy[dt.datetime] = DATETIME_MIN_UTC + DAY,
    max_value: MaybeSearchStrategy[dt.datetime] = DATETIME_MAX_UTC - DAY,
    time_zone: MaybeSearchStrategy[ZoneInfo | timezone] = UTC,
    round_: MathRoundMode | None = None,
    timedelta: dt.timedelta | None = None,
    rel_tol: float | None = None,
    abs_tol: float | None = None,
    valid: bool = False,
) -> dt.datetime:
    """Strategy for generating zoned datetimes."""
    from utilities.whenever import (
        CheckValidZonedDateTimeError,
        check_valid_zoned_datetime,
    )

    min_value_, max_value_ = [draw2(draw, v) for v in [min_value, max_value]]
    time_zone_ = draw2(draw, time_zone)
    if min_value_.tzinfo is None:
        min_value_ = min_value_.replace(tzinfo=time_zone_)
    else:
        with assume_does_not_raise(OverflowError, match="date value out of range"):
            min_value_ = min_value_.astimezone(time_zone_)
    if max_value_.tzinfo is None:
        max_value_ = max_value_.replace(tzinfo=time_zone_)
    else:
        with assume_does_not_raise(OverflowError, match="date value out of range"):
            max_value_ = max_value_.astimezone(time_zone_)
    try:
        datetime = draw(
            plain_datetimes(
                min_value=min_value_.replace(tzinfo=None),
                max_value=max_value_.replace(tzinfo=None),
                round_=round_,
                timedelta=timedelta,
                rel_tol=rel_tol,
                abs_tol=abs_tol,
            )
        )
    except PlainDateTimesError as error:
        raise ZonedDateTimesError(round_=error.round_) from None
    datetime = datetime.replace(tzinfo=time_zone_)
    _ = assume(min_value_ <= datetime <= max_value_)
    if valid:
        with assume_does_not_raise(  # skipif-ci-and-windows
            CheckValidZonedDateTimeError
        ):
            check_valid_zoned_datetime(datetime)
    return datetime


@dataclass(kw_only=True, slots=True)
class ZonedDateTimesError(Exception):
    round_: MathRoundMode

    @override
    def __str__(self) -> str:
        return "Rounding requires a timedelta; got None"


##


@composite
def zoned_datetimes_whenever(
    draw: DrawFn,
    /,
    *,
    min_value: MaybeSearchStrategy[PlainDateTime | ZonedDateTime | None] = None,
    max_value: MaybeSearchStrategy[PlainDateTime | ZonedDateTime | None] = None,
    time_zone: MaybeSearchStrategy[TimeZoneLike] = UTC,
) -> ZonedDateTime:
    """Strategy for generating zoned datetimes."""
    min_value_, max_value_ = [draw2(draw, v) for v in [min_value, max_value]]
    time_zone_ = ensure_time_zone(draw2(draw, time_zone))
    match min_value_:
        case None | PlainDateTime():
            ...
        case ZonedDateTime():
            with assume_does_not_raise(ValueError):
                min_value_ = min_value_.to_tz(time_zone_.key).to_plain()
        case _ as never:
            assert_never(never)
    match max_value_:
        case None | PlainDateTime():
            ...
        case ZonedDateTime():
            with assume_does_not_raise(ValueError):
                max_value_ = max_value_.to_tz(time_zone_.key).to_plain()
        case _ as never:
            assert_never(never)
    plain_datetime = draw(
        plain_datetimes_whenever(min_value=min_value_, max_value=max_value_)
    )
    with assume_does_not_raise(ValueError):
        return plain_datetime.assume_tz(time_zone_.key, disambiguate="raise")


__all__ = [
    "Draw2Error",
    "MaybeSearchStrategy",
    "PlainDateTimesError",
    "Shape",
    "ZonedDateTimesError",
    "assume_does_not_raise",
    "bool_arrays",
    "date_deltas_whenever",
    "date_durations",
    "dates_two_digit_year",
    "dates_whenever",
    "datetime_durations",
    "draw2",
    "float32s",
    "float64s",
    "float_arrays",
    "floats_extra",
    "git_repos",
    "hashables",
    "int32s",
    "int64s",
    "int_arrays",
    "lists_fixed_length",
    "min_and_max_datetimes",
    "months",
    "namespace_mixins",
    "numbers",
    "pairs",
    "paths",
    "plain_datetimes",
    "plain_datetimes",
    "plain_datetimes_whenever",
    "random_states",
    "sentinels",
    "sets_fixed_length",
    "setup_hypothesis_profiles",
    "slices",
    "str_arrays",
    "temp_dirs",
    "temp_paths",
    "text_ascii",
    "text_ascii_lower",
    "text_ascii_upper",
    "text_clean",
    "text_digits",
    "text_printable",
    "time_deltas_whenever",
    "timedeltas_2w",
    "times_whenever",
    "triples",
    "uint32s",
    "uint64s",
    "versions",
    "zoned_datetimes",
    "zoned_datetimes_whenever",
]
