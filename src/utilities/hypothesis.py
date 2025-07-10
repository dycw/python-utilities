from __future__ import annotations

import builtins
import datetime as dt
from contextlib import contextmanager
from dataclasses import dataclass
from enum import Enum, auto
from math import ceil, floor, inf, isclose, isfinite, nan
from os import environ
from pathlib import Path
from re import search
from string import ascii_letters, ascii_lowercase, ascii_uppercase, digits, printable
from subprocess import check_call
from typing import TYPE_CHECKING, Any, Literal, assert_never, cast, overload, override

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
    datetimes,
    floats,
    integers,
    just,
    lists,
    none,
    sampled_from,
    sets,
    text,
    uuids,
)
from hypothesis.utils.conventions import not_set
from whenever import (
    Date,
    DateDelta,
    DateTimeDelta,
    MonthDay,
    PlainDateTime,
    RepeatedTime,
    SkippedTime,
    Time,
    TimeDelta,
    TimeZoneNotFoundError,
    YearMonth,
    ZonedDateTime,
)

from utilities.functions import ensure_int, ensure_str
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
from utilities.pathlib import module_path, temp_cwd
from utilities.platform import IS_WINDOWS
from utilities.sentinel import Sentinel, sentinel
from utilities.tempfile import TEMP_DIR, TemporaryDirectory
from utilities.version import Version
from utilities.whenever import (
    DATE_DELTA_MAX,
    DATE_DELTA_MIN,
    DATE_DELTA_PARSABLE_MAX,
    DATE_DELTA_PARSABLE_MIN,
    DATE_TIME_DELTA_MAX,
    DATE_TIME_DELTA_MIN,
    DATE_TIME_DELTA_PARSABLE_MAX,
    DATE_TIME_DELTA_PARSABLE_MIN,
    DATE_TWO_DIGIT_YEAR_MAX,
    DATE_TWO_DIGIT_YEAR_MIN,
    DAY,
    TIME_DELTA_MAX,
    TIME_DELTA_MIN,
    to_date_time_delta,
    to_days,
    to_nanoseconds,
)
from utilities.zoneinfo import UTC, ensure_time_zone

if TYPE_CHECKING:
    from collections.abc import Collection, Hashable, Iterable, Iterator

    from hypothesis.database import ExampleDatabase
    from libcst import Import, ImportFrom
    from numpy.random import RandomState

    from utilities.numpy import NDArrayB, NDArrayF, NDArrayI, NDArrayO
    from utilities.types import Number, TimeZoneLike


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
def date_deltas(
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
    min_days = to_days(min_value_)
    max_days = to_days(max_value_)
    if draw2(draw, parsable):
        min_days = max(min_days, to_days(DATE_DELTA_PARSABLE_MIN))
        max_days = min(max_days, to_days(DATE_DELTA_PARSABLE_MAX))
    days = draw(integers(min_value=min_days, max_value=max_days))
    return DateDelta(days=days)


##


@composite
def date_time_deltas(
    draw: DrawFn,
    /,
    *,
    min_value: MaybeSearchStrategy[DateTimeDelta | None] = None,
    max_value: MaybeSearchStrategy[DateTimeDelta | None] = None,
    parsable: MaybeSearchStrategy[bool] = False,
) -> DateTimeDelta:
    """Strategy for generating date deltas."""
    min_value_, max_value_ = [draw2(draw, v) for v in [min_value, max_value]]
    match min_value_:
        case None:
            min_value_ = DATE_TIME_DELTA_MIN
        case DateTimeDelta():
            ...
        case _ as never:
            assert_never(never)
    match max_value_:
        case None:
            max_value_ = DATE_TIME_DELTA_MAX
        case DateTimeDelta():
            ...
        case _ as never:
            assert_never(never)
    min_nanos, max_nanos = map(to_nanoseconds, [min_value_, max_value_])
    if draw2(draw, parsable):
        min_nanos = max(min_nanos, to_nanoseconds(DATE_TIME_DELTA_PARSABLE_MIN))
        max_nanos = min(max_nanos, to_nanoseconds(DATE_TIME_DELTA_PARSABLE_MAX))
    nanos = draw(integers(min_value=min_nanos, max_value=max_nanos))
    return to_date_time_delta(nanos)


##


@composite
def dates(
    draw: DrawFn,
    /,
    *,
    min_value: MaybeSearchStrategy[Date | None] = None,
    max_value: MaybeSearchStrategy[Date | None] = None,
    two_digit: MaybeSearchStrategy[bool] = False,
) -> Date:
    """Strategy for generating dates."""
    min_value_, max_value_ = [draw2(draw, v) for v in [min_value, max_value]]
    match min_value_:
        case None:
            min_value_ = Date.MIN
        case Date():
            ...
        case _ as never:
            assert_never(never)
    match max_value_:
        case None:
            max_value_ = Date.MAX
        case Date():
            ...
        case _ as never:
            assert_never(never)
    if draw2(draw, two_digit):
        min_value_ = max(min_value_, DATE_TWO_DIGIT_YEAR_MIN)
        max_value_ = min(max_value_, DATE_TWO_DIGIT_YEAR_MAX)
    min_date, max_date = [d.py_date() for d in [min_value_, max_value_]]
    py_date = draw(hypothesis.strategies.dates(min_value=min_date, max_value=max_date))
    return Date.from_py_date(py_date)


##


@overload
def draw2[T](
    data_or_draw: DataObject | DrawFn,
    maybe_strategy: MaybeSearchStrategy[T],
    /,
    *,
    sentinel: bool = False,
) -> T: ...
@overload
def draw2[T](
    data_or_draw: DataObject | DrawFn,
    maybe_strategy: MaybeSearchStrategy[T | None | Sentinel],
    default: SearchStrategy[T | None],
    /,
    *,
    sentinel: Literal[True],
) -> T | None: ...
@overload
def draw2[T](
    data_or_draw: DataObject | DrawFn,
    maybe_strategy: MaybeSearchStrategy[T | None],
    default: SearchStrategy[T],
    /,
    *,
    sentinel: Literal[False] = False,
) -> T: ...
@overload
def draw2[T](
    data_or_draw: DataObject | DrawFn,
    maybe_strategy: MaybeSearchStrategy[T | None | Sentinel],
    default: SearchStrategy[T] | None = None,
    /,
    *,
    sentinel: bool = False,
) -> T | None: ...
def draw2[T](
    data_or_draw: DataObject | DrawFn,
    maybe_strategy: MaybeSearchStrategy[T | None | Sentinel],
    default: SearchStrategy[T | None] | None = None,
    /,
    *,
    sentinel: bool = False,
) -> T | None:
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
def import_froms(
    draw: DrawFn,
    /,
    *,
    min_depth: MaybeSearchStrategy[int | None] = None,
    max_depth: MaybeSearchStrategy[int | None] = None,
) -> ImportFrom:
    """Strategy for generating import-froms."""
    from utilities.libcst import generate_import_from

    min_depth_, max_depth_ = [draw2(draw, d) for d in [min_depth, max_depth]]
    path = draw(
        paths(
            min_depth=1 if min_depth_ is None else max(min_depth_, 1),
            max_depth=max_depth_,
        )
    )
    module = module_path(path)
    name = draw(text_ascii(min_size=1))
    asname = draw(text_ascii(min_size=1) | none())
    return generate_import_from(module, name, asname=asname)


##


@composite
def imports(
    draw: DrawFn,
    /,
    *,
    min_depth: MaybeSearchStrategy[int | None] = None,
    max_depth: MaybeSearchStrategy[int | None] = None,
) -> Import:
    """Strategy for generating imports."""
    from utilities.libcst import generate_import

    min_depth_, max_depth_ = [draw2(draw, d) for d in [min_depth, max_depth]]
    path = draw(
        paths(
            min_depth=1 if min_depth_ is None else max(min_depth_, 1),
            max_depth=max_depth_,
        )
    )
    module = module_path(path)
    asname = draw(text_ascii(min_size=1) | none())
    return generate_import(module, asname=asname)


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
def lists_fixed_length[T](
    draw: DrawFn,
    strategy: SearchStrategy[T],
    size: MaybeSearchStrategy[int],
    /,
    *,
    unique: MaybeSearchStrategy[bool] = False,
    sorted: MaybeSearchStrategy[bool] = False,  # noqa: A002
) -> list[T]:
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
def month_days(
    draw: DrawFn,
    /,
    *,
    min_value: MaybeSearchStrategy[MonthDay | None] = None,
    max_value: MaybeSearchStrategy[MonthDay | None] = None,
) -> MonthDay:
    """Strategy for generating month-days."""
    min_value_, max_value_ = [draw2(draw, v) for v in [min_value, max_value]]
    match min_value_:
        case None:
            min_value_ = MonthDay.MIN
        case MonthDay():
            ...
        case _ as never:
            assert_never(never)
    match max_value_:
        case None:
            max_value_ = MonthDay.MAX
        case MonthDay():
            ...
        case _ as never:
            assert_never(never)
    min_date, max_date = [m.in_year(2000) for m in [min_value_, max_value_]]
    date = draw(dates(min_value=min_date, max_value=max_date))
    return date.month_day()


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


def pairs[T](
    strategy: SearchStrategy[T],
    /,
    *,
    unique: MaybeSearchStrategy[bool] = False,
    sorted: MaybeSearchStrategy[bool] = False,  # noqa: A002
) -> SearchStrategy[tuple[T, T]]:
    """Strategy for generating pairs of elements."""
    return lists_fixed_length(strategy, 2, unique=unique, sorted=sorted).map(_pairs_map)


def _pairs_map[T](elements: list[T], /) -> tuple[T, T]:
    first, second = elements
    return first, second


##


@composite
def paths(
    draw: DrawFn,
    /,
    *,
    min_depth: MaybeSearchStrategy[int | None] = None,
    max_depth: MaybeSearchStrategy[int | None] = None,
) -> Path:
    """Strategy for generating `Path`s."""
    min_depth_, max_depth_ = [draw2(draw, d) for d in [min_depth, max_depth]]
    parts = draw(
        lists(
            _path_parts(),
            min_size=0 if min_depth_ is None else min_depth_,
            max_size=max_depth_,
        )
    )
    return Path(*parts)


@composite
def _path_parts(draw: DrawFn, /) -> str:
    part = draw(text_ascii(min_size=1, max_size=10))
    reserved = {"AUX", "NUL", "nuL"}
    _ = assume(part not in reserved)
    return part


##


@composite
def plain_datetimes(
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
            min_value_ = PlainDateTime.MIN
        case PlainDateTime():
            ...
        case _ as never:
            assert_never(never)
    match max_value_:
        case None:
            max_value_ = PlainDateTime.MAX
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
def py_datetimes(
    draw: DrawFn, /, *, zoned: MaybeSearchStrategy[bool | None] = None
) -> dt.datetime:
    """Strategy for generating standard library datetimes."""
    zoned_ = draw2(draw, zoned, booleans())
    timezones = just(UTC) if zoned_ else none()
    return draw(
        hypothesis.strategies.datetimes(
            min_value=dt.datetime(2000, 1, 1),  # noqa: DTZ001
            max_value=dt.datetime(2000, 12, 31),  # noqa: DTZ001
            timezones=timezones,
        )
    )


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
def sets_fixed_length[T](
    draw: DrawFn, strategy: SearchStrategy[T], size: MaybeSearchStrategy[int], /
) -> set[T]:
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
def time_deltas(
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
def times(
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
            min_value_ = Time()
        case Time():
            ...
        case _ as never:
            assert_never(never)
    match max_value_:
        case None:
            max_value_ = Time.MAX
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


def triples[T](
    strategy: SearchStrategy[T],
    /,
    *,
    unique: MaybeSearchStrategy[bool] = False,
    sorted: MaybeSearchStrategy[bool] = False,  # noqa: A002
) -> SearchStrategy[tuple[T, T, T]]:
    """Strategy for generating triples of elements."""
    return lists_fixed_length(strategy, 3, unique=unique, sorted=sorted).map(
        _triples_map
    )


def _triples_map[T](elements: list[T], /) -> tuple[T, T, T]:
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
def year_months(
    draw: DrawFn,
    /,
    *,
    min_value: MaybeSearchStrategy[YearMonth | None] = None,
    max_value: MaybeSearchStrategy[YearMonth | None] = None,
    two_digit: MaybeSearchStrategy[bool] = False,
) -> YearMonth:
    """Strategy for generating months."""
    min_value_, max_value_ = [draw2(draw, v) for v in [min_value, max_value]]
    match min_value_:
        case None:
            min_value_ = YearMonth.MIN
        case YearMonth():
            ...
        case _ as never:
            assert_never(never)
    match max_value_:
        case None:
            max_value_ = YearMonth.MAX
        case YearMonth():
            ...
        case _ as never:
            assert_never(never)
    min_date, max_date = [m.on_day(1) for m in [min_value_, max_value_]]
    date = draw(dates(min_value=min_date, max_value=max_date, two_digit=two_digit))
    return date.year_month()


##


@composite
def zoned_datetimes(
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
    plain = draw(plain_datetimes(min_value=min_value_, max_value=max_value_))
    with (
        assume_does_not_raise(RepeatedTime),
        assume_does_not_raise(SkippedTime),
        assume_does_not_raise(TimeZoneNotFoundError),
        assume_does_not_raise(ValueError, match="Resulting datetime is out of range"),
    ):
        zoned = plain.assume_tz(time_zone_.key, disambiguate="raise")
    with assume_does_not_raise(OverflowError, match="date value out of range"):
        if not ((Date.MIN + DAY) <= zoned.date() <= (Date.MAX - DAY)):
            _ = zoned.py_datetime()
    return zoned


__all__ = [
    "Draw2Error",
    "MaybeSearchStrategy",
    "Shape",
    "assume_does_not_raise",
    "bool_arrays",
    "date_deltas",
    "date_time_deltas",
    "dates",
    "draw2",
    "float32s",
    "float64s",
    "float_arrays",
    "floats_extra",
    "git_repos",
    "hashables",
    "import_froms",
    "imports",
    "int32s",
    "int64s",
    "int_arrays",
    "lists_fixed_length",
    "month_days",
    "numbers",
    "pairs",
    "paths",
    "plain_datetimes",
    "py_datetimes",
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
    "time_deltas",
    "times",
    "triples",
    "uint32s",
    "uint64s",
    "versions",
    "year_months",
    "zoned_datetimes",
]
