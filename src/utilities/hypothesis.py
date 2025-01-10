from __future__ import annotations

import builtins
import datetime as dt
from collections.abc import (
    AsyncIterator,
    Collection,
    Hashable,
    Iterable,
    Iterator,
    Sequence,
)
from contextlib import (
    AbstractAsyncContextManager,
    asynccontextmanager,
    contextmanager,
    suppress,
)
from datetime import timezone
from enum import Enum, auto
from functools import partial
from math import ceil, floor, inf, isclose, isfinite, nan
from os import environ
from pathlib import Path
from re import search
from shutil import move, rmtree
from string import ascii_letters, ascii_lowercase, ascii_uppercase, digits, printable
from subprocess import check_call
from typing import TYPE_CHECKING, Any, Protocol, TypeVar, assert_never, cast, overload
from zoneinfo import ZoneInfo

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

from utilities.datetime import (
    MAX_MONTH,
    MIN_MONTH,
    Month,
    date_duration_to_int,
    date_duration_to_timedelta,
    date_to_month,
    datetime_duration_to_float,
    datetime_duration_to_timedelta,
    get_now,
)
from utilities.functions import ensure_str, max_nullable, min_nullable
from utilities.math import (
    MAX_INT32,
    MAX_INT64,
    MAX_UINT32,
    MAX_UINT64,
    MIN_INT32,
    MIN_INT64,
    MIN_UINT32,
    MIN_UINT64,
)
from utilities.pathlib import temp_cwd
from utilities.platform import IS_WINDOWS
from utilities.tempfile import TEMP_DIR, TemporaryDirectory
from utilities.version import Version
from utilities.zoneinfo import UTC

if TYPE_CHECKING:
    from hypothesis.database import ExampleDatabase
    from numpy.random import RandomState
    from sqlalchemy.ext.asyncio import AsyncEngine

    from utilities.numpy import NDArrayB, NDArrayF, NDArrayI, NDArrayO
    from utilities.redis import _TestRedis
    from utilities.sqlalchemy import Dialect, TableOrORMInstOrClass
    from utilities.types import Duration, Number


_T = TypeVar("_T")
MaybeSearchStrategy = _T | SearchStrategy[_T]
Shape = int | tuple[int, ...]


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
    _draw: DrawFn,
    /,
    *,
    shape: MaybeSearchStrategy[Shape] | None = None,
    fill: SearchStrategy[Any] | None = None,
    unique: MaybeSearchStrategy[bool] = False,
) -> NDArrayB:
    """Strategy for generating arrays of booleans."""
    from hypothesis.extra.numpy import array_shapes, arrays

    draw = lift_draw(_draw)
    shape_use = array_shapes() if shape is None else shape
    strategy: SearchStrategy[NDArrayB] = arrays(
        bool, draw(shape_use), elements=booleans(), fill=fill, unique=draw(unique)
    )
    return draw(strategy)


##


@composite
def date_durations(
    _draw: DrawFn,
    /,
    *,
    min_int: MaybeSearchStrategy[int] | None = None,
    max_int: MaybeSearchStrategy[int] | None = None,
    min_timedelta: MaybeSearchStrategy[dt.timedelta] | None = None,
    max_timedelta: MaybeSearchStrategy[dt.timedelta] | None = None,
    two_way: bool = False,
) -> Duration:
    """Strategy for generating datetime durations."""
    draw = lift_draw(_draw)
    min_int_, max_int_, min_timedelta_, max_timedelta_ = (
        draw(min_int),
        draw(max_int),
        draw(min_timedelta),
        draw(max_timedelta),
    )
    min_parts: Sequence[dt.timedelta | None] = [dt.timedelta.min, min_timedelta_]
    if min_int_ is not None:
        with assume_does_not_raise(OverflowError):
            min_parts.append(date_duration_to_timedelta(min_int_))
    if two_way:
        from utilities.whenever import MIN_TWO_WAY_TIMEDELTA

        min_parts.append(MIN_TWO_WAY_TIMEDELTA)
    min_timedelta_use = max_nullable(min_parts)
    max_parts: Sequence[dt.timedelta | None] = [dt.timedelta.max, max_timedelta_]
    if max_int_ is not None:
        with assume_does_not_raise(OverflowError):
            max_parts.append(date_duration_to_timedelta(max_int_))
    if two_way:
        from utilities.whenever import MAX_TWO_WAY_TIMEDELTA

        max_parts.append(MAX_TWO_WAY_TIMEDELTA)
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
    return _draw(st_integers | st_floats | st_timedeltas)


def _round_timedelta(timedelta: dt.timedelta, /) -> dt.timedelta:
    return dt.timedelta(days=timedelta.days)


def _is_between_timedelta(
    timedelta: dt.timedelta, /, *, min_: dt.timedelta, max_: dt.timedelta
) -> bool:
    return min_ <= timedelta <= max_


##


@composite
def datetime_durations(
    _draw: DrawFn,
    /,
    *,
    min_number: MaybeSearchStrategy[Number] | None = None,
    max_number: MaybeSearchStrategy[Number] | None = None,
    min_timedelta: MaybeSearchStrategy[dt.timedelta] | None = None,
    max_timedelta: MaybeSearchStrategy[dt.timedelta] | None = None,
    two_way: bool = False,
) -> Duration:
    """Strategy for generating datetime durations."""
    draw = lift_draw(_draw)
    min_number_, max_number_, min_timedelta_, max_timedelta_ = (
        draw(min_number),
        draw(max_number),
        draw(min_timedelta),
        draw(max_timedelta),
    )
    min_parts: list[dt.timedelta] = [dt.timedelta.min]
    if min_number_ is not None:
        with assume_does_not_raise(OverflowError):
            min_parts.append(datetime_duration_to_timedelta(min_number_))
    if min_timedelta_ is not None:
        min_parts.append(min_timedelta_)
    if two_way:
        from utilities.whenever import MIN_TWO_WAY_TIMEDELTA

        min_parts.append(MIN_TWO_WAY_TIMEDELTA)
    min_timedelta_use = max(min_parts)
    max_parts: list[dt.timedelta] = [dt.timedelta.max]
    if max_number_ is not None:
        with assume_does_not_raise(OverflowError):
            max_parts.append(datetime_duration_to_timedelta(max_number_))
    if max_timedelta_ is not None:
        max_parts.append(max_timedelta_)
    if two_way:
        from utilities.whenever import MAX_TWO_WAY_TIMEDELTA

        max_parts.append(MAX_TWO_WAY_TIMEDELTA)
    max_timedelta_use = min(max_parts)
    _ = assume(min_timedelta_use <= max_timedelta_use)
    min_float_use, max_float_use = map(
        datetime_duration_to_float, [min_timedelta_use, max_timedelta_use]
    )
    _ = assume(min_float_use <= max_float_use)
    st_numbers = numbers(min_value=min_float_use, max_value=max_float_use)
    st_timedeltas = timedeltas(min_value=min_timedelta_use, max_value=max_timedelta_use)
    return _draw(st_numbers | st_timedeltas)


##


@composite
def float_arrays(
    _draw: DrawFn,
    /,
    *,
    shape: MaybeSearchStrategy[Shape] | None = None,
    min_value: MaybeSearchStrategy[float | None] = None,
    max_value: MaybeSearchStrategy[float | None] = None,
    allow_nan: MaybeSearchStrategy[bool] = False,
    allow_inf: MaybeSearchStrategy[bool] = False,
    allow_pos_inf: MaybeSearchStrategy[bool] = False,
    allow_neg_inf: MaybeSearchStrategy[bool] = False,
    integral: MaybeSearchStrategy[bool] = False,
    fill: SearchStrategy[Any] | None = None,
    unique: MaybeSearchStrategy[bool] = False,
) -> NDArrayF:
    """Strategy for generating arrays of floats."""
    from hypothesis.extra.numpy import array_shapes, arrays

    draw = lift_draw(_draw)
    shape_use = array_shapes() if shape is None else shape
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
        float, draw(shape_use), elements=elements, fill=fill, unique=draw(unique)
    )
    return draw(strategy)


##


@composite
def floats_extra(
    _draw: DrawFn,
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
    draw = lift_draw(_draw)
    min_value_, max_value_ = draw(min_value), draw(max_value)
    elements = floats(
        min_value=min_value_,
        max_value=max_value_,
        allow_nan=False,
        allow_infinity=False,
    )
    if draw(allow_nan):
        elements |= just(nan)
    if draw(allow_inf):
        elements |= sampled_from([inf, -inf])
    if draw(allow_pos_inf):
        elements |= just(inf)
    if draw(allow_neg_inf):
        elements |= just(-inf)
    element = draw(elements)
    if isfinite(element) and draw(integral):
        candidates = [floor(element), ceil(element)]
        if min_value_ is not None:
            candidates = [c for c in candidates if c >= min_value_]
        if max_value_ is not None:
            candidates = [c for c in candidates if c <= max_value_]
        _ = assume(len(candidates) >= 1)
        element = draw(sampled_from(candidates))
        return float(element)
    return element


##


@composite
def git_repos(
    _draw: DrawFn,
    /,
    *,
    branch: MaybeSearchStrategy[str | None] = None,
    remote: MaybeSearchStrategy[str | None] = None,
    git_version: MaybeSearchStrategy[Version | None] = None,
    hatch_version: MaybeSearchStrategy[Version | None] = None,
) -> Path:
    draw = lift_draw(_draw)
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
        if (branch_ := draw(branch)) is not None:
            _ = check_call(["git", "checkout", "-b", branch_])
        if (remote_ := draw(remote)) is not None:
            _ = check_call(["git", "remote", "add", "origin", remote_])
        if (git_version_ := draw(git_version)) is not None:
            _ = check_call(["git", "tag", str(git_version_), "master"])
        if (hatch_version_ := draw(hatch_version)) is not None:
            _ = check_call(["hatch", "new", "package"])
            package = path.joinpath("package")
            for p in package.iterdir():
                move(p, p.parent.with_name(p.name))
            rmtree(package)
            if (hatch_version_ > Version(0, 0, 1)) and (hatch_version_.suffix is None):
                _ = check_call(["hatch", "version", str(hatch_version_)])
    return path


##


def hashables() -> SearchStrategy[Hashable]:
    """Strategy for generating hashable elements."""
    return booleans() | integers() | none() | text_ascii()


##


@composite
def int_arrays(
    _draw: DrawFn,
    /,
    *,
    shape: MaybeSearchStrategy[Shape] | None = None,
    min_value: MaybeSearchStrategy[int] = MIN_INT64,
    max_value: MaybeSearchStrategy[int] = MAX_INT64,
    fill: SearchStrategy[Any] | None = None,
    unique: MaybeSearchStrategy[bool] = False,
) -> NDArrayI:
    """Strategy for generating arrays of ints."""
    from hypothesis.extra.numpy import array_shapes, arrays
    from numpy import int64

    draw = lift_draw(_draw)
    shape_use = array_shapes() if shape is None else shape
    elements = int64s(min_value=min_value, max_value=max_value)
    strategy: SearchStrategy[NDArrayI] = arrays(
        int64, draw(shape_use), elements=elements, fill=fill, unique=draw(unique)
    )
    return draw(strategy)


##


@composite
def int32s(
    _draw: DrawFn,
    /,
    *,
    min_value: MaybeSearchStrategy[int] = MIN_INT32,
    max_value: MaybeSearchStrategy[int] = MAX_INT32,
) -> int:
    """Strategy for generating int32s."""
    draw = lift_draw(_draw)
    min_value_, max_value_ = draw(min_value), draw(max_value)
    min_value_ = max(min_value_, MIN_INT32)
    max_value_ = min(max_value_, MAX_INT32)
    return draw(integers(min_value_, max_value_))


@composite
def int64s(
    _draw: DrawFn,
    /,
    *,
    min_value: MaybeSearchStrategy[int] = MIN_INT64,
    max_value: MaybeSearchStrategy[int] = MAX_INT64,
) -> int:
    """Strategy for generating int64s."""
    draw = lift_draw(_draw)
    min_value_, max_value_ = draw(min_value), draw(max_value)
    min_value_ = max(min_value_, MIN_INT64)
    max_value_ = min(max_value_, MAX_INT64)
    return draw(integers(min_value_, max_value_))


##


_MDF = TypeVar("_MDF")


class _MaybeDrawFn(Protocol):
    @overload
    def __call__(self, obj: SearchStrategy[_MDF], /) -> _MDF: ...
    @overload
    def __call__(self, obj: MaybeSearchStrategy[_MDF], /) -> _MDF: ...
    def __call__(self, obj: MaybeSearchStrategy[_MDF], /) -> _MDF:
        raise NotImplementedError(obj)  # pragma: no cover


def lift_data(data: DataObject, /) -> _MaybeDrawFn:
    """Lift the `data` object to handle non-`SearchStrategy` types."""

    def func(obj: MaybeSearchStrategy[_MDF], /) -> _MDF:
        return data.draw(obj) if isinstance(obj, SearchStrategy) else obj

    return cast(_MaybeDrawFn, func)


def lift_draw(draw: DrawFn, /) -> _MaybeDrawFn:
    """Lift the `draw` function to handle non-`SearchStrategy` types."""

    def func(obj: MaybeSearchStrategy[_MDF], /) -> _MDF:
        return draw(obj) if isinstance(obj, SearchStrategy) else obj

    return cast(_MaybeDrawFn, func)


##


@composite
def lists_fixed_length(
    _draw: DrawFn,
    strategy: SearchStrategy[_T],
    size: MaybeSearchStrategy[int],
    /,
    *,
    unique: MaybeSearchStrategy[bool] = False,
    sorted: MaybeSearchStrategy[bool] = False,  # noqa: A002
) -> list[_T]:
    """Strategy for generating lists of a fixed length."""
    draw = lift_draw(_draw)
    size_ = draw(size)
    elements = draw(
        lists(strategy, min_size=size_, max_size=size_, unique=draw(unique))
    )
    if draw(sorted):
        return builtins.sorted(cast(Iterable[Any], elements))
    return elements


##


@composite
def months(
    _draw: DrawFn,
    /,
    *,
    min_value: MaybeSearchStrategy[Month] = MIN_MONTH,
    max_value: MaybeSearchStrategy[Month] = MAX_MONTH,
) -> Month:
    """Strategy for generating datetimes with the UTC timezone."""
    draw = lift_draw(_draw)
    min_date = draw(min_value).to_date()
    max_date = draw(max_value).to_date()
    date = draw(dates(min_value=min_date, max_value=max_date))
    return date_to_month(date)


##


@composite
def numbers(
    _draw: DrawFn,
    /,
    *,
    min_value: MaybeSearchStrategy[Number] | None = None,
    max_value: MaybeSearchStrategy[Number] | None = None,
) -> int | float:
    """Strategy for generating numbers."""
    draw = lift_draw(_draw)
    min_value_, max_value_ = draw(min_value), draw(max_value)
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
    return lists(text_ascii(min_size=1, max_size=10), max_size=10).map(
        lambda parts: Path(*parts)
    )


##


@composite
def random_states(
    _draw: DrawFn, /, *, seed: MaybeSearchStrategy[int | None] = None
) -> RandomState:
    """Strategy for generating `numpy` random states."""
    from numpy.random import RandomState

    draw = lift_draw(_draw)
    seed_ = draw(seed)
    seed_use = draw(integers(0, MAX_UINT32)) if seed_ is None else seed_
    return RandomState(seed=seed_use)


##


@composite
def sets_fixed_length(
    _draw: DrawFn, strategy: SearchStrategy[_T], size: MaybeSearchStrategy[int], /
) -> set[_T]:
    """Strategy for generating lists of a fixed length."""
    draw = lift_draw(_draw)
    size_ = draw(size)
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
        def verbosity(self) -> Verbosity | None:
            match self:
                case Profile.dev | Profile.default | Profile.ci:
                    return Verbosity.normal
                case Profile.debug:
                    return Verbosity.debug
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
            report_multiple_bugs=True,
            deadline=None,
            print_blob=True,
            suppress_health_check=suppress_health_check,
            verbosity=profile.verbosity,
        )
    settings.load_profile(Profile.default.name)


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
    max_examples = max(round(frac * curr.max_examples), 1)
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
    _draw: DrawFn,
    iter_len: MaybeSearchStrategy[int],
    /,
    *,
    slice_len: MaybeSearchStrategy[int | None] = None,
) -> slice:
    """Strategy for generating continuous slices from an iterable."""
    draw = lift_draw(_draw)
    iter_len_ = draw(iter_len)
    if (slice_len_ := draw(slice_len)) is None:
        slice_len_ = draw(integers(0, iter_len_))
    elif not 0 <= slice_len_ <= iter_len_:
        msg = f"Slice length {slice_len_} exceeds iterable length {iter_len_}"
        raise InvalidArgument(msg)
    start = draw(integers(0, iter_len_ - slice_len_))
    stop = start + slice_len_
    return slice(start, stop)


##


_STRATEGY_DIALECTS: list[Dialect] = ["sqlite", "postgresql"]
_SQLALCHEMY_ENGINE_DIALECTS = sampled_from(_STRATEGY_DIALECTS)


async def sqlalchemy_engines(
    _data: DataObject,
    /,
    *tables_or_orms: TableOrORMInstOrClass,
    dialect: MaybeSearchStrategy[Dialect] = _SQLALCHEMY_ENGINE_DIALECTS,
) -> AsyncEngine:
    """Strategy for generating sqlalchemy engines."""
    from utilities.sqlalchemy import create_async_engine

    draw = lift_data(_data)
    dialect_: Dialect = draw(dialect)
    if "CI" in environ:  # pragma: no cover
        _ = assume(dialect_ == "sqlite")
    match dialect_:
        case "sqlite":
            temp_path = draw(temp_paths())
            path = Path(temp_path, "db.sqlite")
            engine = create_async_engine("sqlite+aiosqlite", database=str(path))

            class EngineWithPath(type(engine)): ...

            engine_with_path = EngineWithPath(engine.sync_engine)
            cast(Any, engine_with_path).temp_path = temp_path  # keep `temp_path` alive
            return engine_with_path
        case "postgresql":  # skipif-ci-and-not-linux
            from utilities.sqlalchemy import ensure_tables_dropped

            engine = create_async_engine(
                "postgresql+asyncpg", host="localhost", port=5432, database="testing"
            )
            await ensure_tables_dropped(engine, *tables_or_orms)
            return engine
        case _:  # pragma: no cover
            raise NotImplementedError(dialect)


##


@composite
def str_arrays(
    _draw: DrawFn,
    /,
    *,
    shape: MaybeSearchStrategy[Shape] | None = None,
    min_size: MaybeSearchStrategy[int] = 0,
    max_size: MaybeSearchStrategy[int | None] = None,
    allow_none: MaybeSearchStrategy[bool] = False,
    fill: SearchStrategy[Any] | None = None,
    unique: MaybeSearchStrategy[bool] = False,
) -> NDArrayO:
    """Strategy for generating arrays of strings."""
    from hypothesis.extra.numpy import array_shapes, arrays

    draw = lift_draw(_draw)
    shape_use = array_shapes() if shape is None else shape
    elements = text_ascii(min_size=min_size, max_size=max_size)
    if draw(allow_none):
        elements |= none()
    strategy: SearchStrategy[NDArrayO] = arrays(
        object, draw(shape_use), elements=elements, fill=fill, unique=draw(unique)
    )
    return draw(strategy)


##


_TEMP_DIR_HYPOTHESIS = Path(TEMP_DIR, "hypothesis")


@composite
def temp_dirs(_draw: DrawFn, /) -> TemporaryDirectory:
    """Search strategy for temporary directories."""
    _TEMP_DIR_HYPOTHESIS.mkdir(exist_ok=True)
    uuid = _draw(uuids())
    return TemporaryDirectory(
        prefix=f"{uuid}__", dir=_TEMP_DIR_HYPOTHESIS, ignore_cleanup_errors=IS_WINDOWS
    )


##


@composite
def temp_paths(_draw: DrawFn, /) -> Path:
    """Search strategy for paths to temporary directories."""
    temp_dir = _draw(temp_dirs())
    root = temp_dir.path
    cls = type(root)

    class SubPath(cls):
        _temp_dir = temp_dir

    return SubPath(root)


##


@composite
def text_ascii(
    _draw: DrawFn,
    /,
    *,
    min_size: MaybeSearchStrategy[int] = 0,
    max_size: MaybeSearchStrategy[int | None] = None,
) -> str:
    """Strategy for generating ASCII text."""
    draw = lift_draw(_draw)
    alphabet = characters(whitelist_categories=[], whitelist_characters=ascii_letters)
    return draw(text(alphabet, min_size=draw(min_size), max_size=draw(max_size)))


@composite
def text_ascii_lower(
    _draw: DrawFn,
    /,
    *,
    min_size: MaybeSearchStrategy[int] = 0,
    max_size: MaybeSearchStrategy[int | None] = None,
) -> str:
    """Strategy for generating ASCII lower-case text."""
    draw = lift_draw(_draw)
    alphabet = characters(whitelist_categories=[], whitelist_characters=ascii_lowercase)
    return draw(text(alphabet, min_size=draw(min_size), max_size=draw(max_size)))


@composite
def text_ascii_upper(
    _draw: DrawFn,
    /,
    *,
    min_size: MaybeSearchStrategy[int] = 0,
    max_size: MaybeSearchStrategy[int | None] = None,
) -> str:
    """Strategy for generating ASCII upper-case text."""
    draw = lift_draw(_draw)
    alphabet = characters(whitelist_categories=[], whitelist_characters=ascii_uppercase)
    return draw(text(alphabet, min_size=draw(min_size), max_size=draw(max_size)))


@composite
def text_clean(
    _draw: DrawFn,
    /,
    *,
    min_size: MaybeSearchStrategy[int] = 0,
    max_size: MaybeSearchStrategy[int | None] = None,
) -> str:
    """Strategy for generating clean text."""
    draw = lift_draw(_draw)
    alphabet = characters(blacklist_categories=["Z", "C"])
    return draw(text(alphabet, min_size=draw(min_size), max_size=draw(max_size)))


@composite
def text_digits(
    _draw: DrawFn,
    /,
    *,
    min_size: MaybeSearchStrategy[int] = 0,
    max_size: MaybeSearchStrategy[int | None] = None,
) -> str:
    """Strategy for generating ASCII text."""
    draw = lift_draw(_draw)
    alphabet = characters(whitelist_categories=[], whitelist_characters=digits)
    return draw(text(alphabet, min_size=draw(min_size), max_size=draw(max_size)))


@composite
def text_printable(
    _draw: DrawFn,
    /,
    *,
    min_size: MaybeSearchStrategy[int] = 0,
    max_size: MaybeSearchStrategy[int | None] = None,
) -> str:
    """Strategy for generating printable text."""
    draw = lift_draw(_draw)
    alphabet = characters(whitelist_categories=[], whitelist_characters=printable)
    return draw(text(alphabet, min_size=draw(min_size), max_size=draw(max_size)))


##


@composite
def timedeltas_2w(
    _draw: DrawFn,
    /,
    *,
    min_value: MaybeSearchStrategy[dt.timedelta] = dt.timedelta.min,
    max_value: MaybeSearchStrategy[dt.timedelta] = dt.timedelta.max,
) -> dt.timedelta:
    """Strategy for generating timedeltas which can be se/deserialized."""
    from utilities.whenever import MAX_TWO_WAY_TIMEDELTA, MIN_TWO_WAY_TIMEDELTA

    draw = lift_draw(_draw)
    min_value_ = max(draw(min_value), MIN_TWO_WAY_TIMEDELTA)
    max_value_ = min(draw(max_value), MAX_TWO_WAY_TIMEDELTA)
    return draw(timedeltas(min_value=min_value_, max_value=max_value_))


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
    _draw: DrawFn,
    /,
    *,
    min_value: MaybeSearchStrategy[int] = MIN_UINT32,
    max_value: MaybeSearchStrategy[int] = MAX_UINT32,
) -> int:
    """Strategy for generating uint32s."""
    draw = lift_draw(_draw)
    min_value_, max_value_ = draw(min_value), draw(max_value)
    min_value_ = max(min_value_, MIN_UINT32)
    max_value_ = min(max_value_, MAX_UINT32)
    return draw(integers(min_value_, max_value_))


@composite
def uint64s(
    _draw: DrawFn,
    /,
    *,
    min_value: MaybeSearchStrategy[int] = MIN_UINT64,
    max_value: MaybeSearchStrategy[int] = MAX_UINT64,
) -> int:
    """Strategy for generating uint64s."""
    draw = lift_draw(_draw)
    min_value_, max_value_ = draw(min_value), draw(max_value)
    min_value_ = max(min_value_, MIN_UINT64)
    max_value_ = min(max_value_, MAX_UINT64)
    return draw(integers(min_value_, max_value_))


##


@composite
def versions(_draw: DrawFn, /, *, suffix: MaybeSearchStrategy[bool] = False) -> Version:
    """Strategy for generating versions."""
    draw = lift_draw(_draw)
    major, minor, patch = draw(triples(integers(min_value=0)))
    _ = assume((major >= 1) or (minor >= 1) or (patch >= 1))
    suffix_use = draw(text_ascii(min_size=1)) if draw(suffix) else None
    return Version(major=major, minor=minor, patch=patch, suffix=suffix_use)


##


def yield_test_redis(data: DataObject, /) -> AbstractAsyncContextManager[_TestRedis]:
    """Strategy for generating test redis clients."""
    from redis.exceptions import ResponseError  # skipif-ci-and-not-linux
    from redis.typing import KeyT  # skipif-ci-and-not-linux

    from utilities.redis import _TestRedis, yield_redis  #  skipif-ci-and-not-linux

    draw = lift_data(data)  # skipif-ci-and-not-linux
    now = get_now(time_zone="local")  # skipif-ci-and-not-linux
    uuid = draw(uuids())  # skipif-ci-and-not-linux
    key = f"{now}_{uuid}"  # skipif-ci-and-not-linux

    @asynccontextmanager
    async def func() -> AsyncIterator[_TestRedis]:  # skipif-ci-and-not-linux
        async with yield_redis(db=15) as redis:  # skipif-ci-and-not-linux
            keys = cast(list[KeyT], await redis.keys(pattern=f"{key}_*"))
            with suppress(ResponseError):
                _ = await redis.delete(*keys)
            yield _TestRedis(redis=redis, timestamp=now, uuid=uuid, key=key)
            keys = cast(list[KeyT], await redis.keys(pattern=f"{key}_*"))
            with suppress(ResponseError):
                _ = await redis.delete(*keys)

    return func()  # skipif-ci-and-not-linux


##


_ZONED_DATETIMES_LEFT_MOST = ZoneInfo("Asia/Manila")
_ZONED_DATETIMES_RIGHT_MOST = ZoneInfo("Pacific/Kiritimati")


@composite
def zoned_datetimes(
    _draw: DrawFn,
    /,
    *,
    min_value: MaybeSearchStrategy[dt.datetime] = dt.datetime.min,
    max_value: MaybeSearchStrategy[dt.datetime] = dt.datetime.max,
    time_zone: MaybeSearchStrategy[ZoneInfo | timezone] = UTC,
    valid: bool = False,
) -> dt.datetime:
    """Strategy for generating zoned datetimes."""
    from utilities.whenever import (
        CheckValidZonedDateimeError,
        check_valid_zoned_datetime,
    )

    draw = lift_draw(_draw)
    min_value_, max_value_, time_zone_ = (
        draw(min_value),
        draw(max_value),
        draw(time_zone),
    )
    if min_value_.tzinfo is not None:
        with assume_does_not_raise(OverflowError, match="date value out of range"):
            min_value_ = min_value_.astimezone(time_zone_)
        min_value_ = min_value_.replace(tzinfo=None)
    if max_value_.tzinfo is not None:
        with assume_does_not_raise(OverflowError, match="date value out of range"):
            max_value_ = max_value_.astimezone(time_zone_)
        max_value_ = max_value_.replace(tzinfo=None)
    strategy = datetimes(
        min_value=min_value_, max_value=max_value_, timezones=just(time_zone_)
    )
    datetime = draw(strategy)
    with assume_does_not_raise(OverflowError, match="date value out of range"):
        _ = datetime.astimezone(_ZONED_DATETIMES_LEFT_MOST)  # for dt.datetime.min
    with assume_does_not_raise(OverflowError, match="date value out of range"):
        _ = datetime.astimezone(  # for dt.datetime.max
            _ZONED_DATETIMES_RIGHT_MOST
        )
    result = datetime.astimezone(time_zone_)
    if valid:
        with assume_does_not_raise(  # skipif-ci-and-windows
            CheckValidZonedDateimeError
        ):
            check_valid_zoned_datetime(result)
    return result


__all__ = [
    "MaybeSearchStrategy",
    "Shape",
    "assume_does_not_raise",
    "bool_arrays",
    "date_durations",
    "datetime_durations",
    "float_arrays",
    "floats_extra",
    "git_repos",
    "hashables",
    "int32s",
    "int64s",
    "int_arrays",
    "lift_data",
    "lift_draw",
    "lists_fixed_length",
    "months",
    "numbers",
    "pairs",
    "paths",
    "random_states",
    "sets_fixed_length",
    "setup_hypothesis_profiles",
    "slices",
    "sqlalchemy_engines",
    "str_arrays",
    "temp_dirs",
    "temp_paths",
    "text_ascii",
    "text_ascii_lower",
    "text_ascii_upper",
    "text_clean",
    "text_digits",
    "text_printable",
    "timedeltas_2w",
    "triples",
    "uint32s",
    "uint64s",
    "versions",
    "yield_test_redis",
    "zoned_datetimes",
]
