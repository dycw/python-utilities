from __future__ import annotations

import builtins
import datetime as dt
from collections.abc import Hashable, Iterable, Iterator, Mapping
from contextlib import contextmanager
from enum import Enum, auto
from math import ceil, floor, inf, isfinite, nan
from os import environ
from pathlib import Path
from re import search
from string import ascii_letters, printable
from subprocess import run
from typing import TYPE_CHECKING, Any, Protocol, TypeVar, cast, overload

from hypothesis import HealthCheck, Phase, Verbosity, assume, settings
from hypothesis.errors import InvalidArgument
from hypothesis.strategies import (
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
    text,
    uuids,
)
from typing_extensions import assert_never

from utilities.pathlib import temp_cwd
from utilities.platform import IS_WINDOWS
from utilities.tempfile import TEMP_DIR, TemporaryDirectory
from utilities.text import ensure_str
from utilities.zoneinfo import UTC

if TYPE_CHECKING:
    from pandas import Timestamp
    from semver import Version
    from sqlalchemy import Engine, MetaData

    from utilities.math import FloatFinPos, IntNonNeg
    from utilities.numpy import NDArrayA, NDArrayB, NDArrayF, NDArrayI, NDArrayO
    from utilities.pandas import IndexA, IndexI, IndexS
    from utilities.xarray import DataArrayB, DataArrayF, DataArrayI, DataArrayO


_T = TypeVar("_T")
MaybeSearchStrategy = _T | SearchStrategy[_T]
Shape = int | tuple[int, ...]
_INDEX_LENGTHS = integers(0, 10)


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


@composite
def bool_data_arrays(
    _draw: DrawFn,
    indexes: MaybeSearchStrategy[Mapping[str, IndexA]] | None = None,
    /,
    *,
    fill: SearchStrategy[Any] | None = None,
    unique: MaybeSearchStrategy[bool] = False,
    name: MaybeSearchStrategy[str | None] = None,
    **indexes_kwargs: MaybeSearchStrategy[IndexA],
) -> DataArrayB:
    """Strategy for generating data arrays of booleans."""
    from xarray import DataArray

    draw = lift_draw(_draw)
    indexes_ = draw(_merge_into_dict_of_indexes(indexes, **indexes_kwargs))
    shape = tuple(map(len, indexes_.values()))
    values = draw(bool_arrays(shape=shape, fill=fill, unique=unique))
    return DataArray(data=values, coords=indexes_, dims=list(indexes_), name=draw(name))


@composite
def datetimes_utc(
    _draw: DrawFn,
    /,
    *,
    min_value: MaybeSearchStrategy[dt.datetime] = dt.datetime.min,
    max_value: MaybeSearchStrategy[dt.datetime] = dt.datetime.max,
) -> dt.datetime:
    """Strategy for generating datetimes with the UTC timezone."""
    draw = lift_draw(_draw)
    return draw(
        datetimes(
            min_value=draw(min_value).replace(tzinfo=None),
            max_value=draw(max_value).replace(tzinfo=None),
            timezones=just(UTC),
        )
    )


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


@composite
def concatenated_arrays(
    _draw: DrawFn,
    strategy: SearchStrategy[NDArrayA],
    size: MaybeSearchStrategy[IntNonNeg],
    fallback: Shape,
    /,
    *,
    dtype: Any = float,
) -> NDArrayA:
    """Strategy for generating arrays from lower-dimensional strategies."""
    from numpy import concatenate, expand_dims, zeros

    from utilities.numpy import (
        EmptyNumpyConcatenateError,
        redirect_empty_numpy_concatenate,
    )

    draw = lift_draw(_draw)
    size_ = draw(size)
    arrays = draw(lists_fixed_length(strategy, size_))
    expanded = [expand_dims(array, axis=0) for array in arrays]
    try:
        with redirect_empty_numpy_concatenate():
            return concatenate(expanded)
    except EmptyNumpyConcatenateError:
        shape = (size_, fallback) if isinstance(fallback, int) else (size_, *fallback)
        return zeros(shape, dtype=dtype)


@composite
def dates_pd(
    _draw: DrawFn,
    /,
    *,
    min_value: MaybeSearchStrategy[dt.date] | None = None,
    max_value: MaybeSearchStrategy[dt.date] | None = None,
) -> dt.date:
    """Strategy for generating dates which can become Timestamps."""
    from utilities.pandas import TIMESTAMP_MAX_AS_DATE, TIMESTAMP_MIN_AS_DATE

    min_value_use = TIMESTAMP_MIN_AS_DATE if min_value is None else min_value
    max_value_use = TIMESTAMP_MAX_AS_DATE if max_value is None else max_value
    draw = lift_draw(_draw)
    return draw(dates(min_value=draw(min_value_use), max_value=draw(max_value_use)))


@composite
def datetimes_pd(
    _draw: DrawFn,
    /,
    *,
    min_value: MaybeSearchStrategy[dt.datetime] | None = None,
    max_value: MaybeSearchStrategy[dt.datetime] | None = None,
) -> dt.datetime:
    """Strategy for generating datetimes which can become Timestamps."""
    from utilities.pandas import TIMESTAMP_MAX_AS_DATETIME, TIMESTAMP_MIN_AS_DATETIME

    draw = lift_draw(_draw)
    min_value_use = TIMESTAMP_MIN_AS_DATETIME if min_value is None else min_value
    max_value_use = TIMESTAMP_MAX_AS_DATETIME if max_value is None else max_value
    datetime = draw(
        datetimes(
            min_value=draw(min_value_use).replace(tzinfo=None),
            max_value=draw(max_value_use).replace(tzinfo=None),
        )
    )
    return datetime.replace(tzinfo=UTC)


@composite
def draw_text(
    _draw: DrawFn,
    alphabet: MaybeSearchStrategy[str],
    /,
    *,
    min_size: MaybeSearchStrategy[int] = 0,
    max_size: MaybeSearchStrategy[int | None] = None,
    disallow_na: MaybeSearchStrategy[bool] = False,
) -> str:
    """Draw from a text-generating strategy."""
    draw = lift_draw(_draw)
    drawn = draw(text(alphabet, min_size=draw(min_size), max_size=draw(max_size)))
    if draw(disallow_na):
        _ = assume(drawn != "NA")
    return drawn


@composite
def dicts_of_indexes(
    _draw: DrawFn,
    /,
    *,
    min_dims: int = 1,
    max_dims: int | None = None,
    min_side: int = 1,
    max_side: int | None = None,
) -> dict[str, IndexI]:
    """Strategy for generating dictionaries of indexes."""
    from hypothesis.extra.numpy import array_shapes

    draw = lift_draw(_draw)
    shape = draw(
        array_shapes(
            min_dims=min_dims, max_dims=max_dims, min_side=min_side, max_side=max_side
        )
    )
    ndims = len(shape)
    dims = draw(lists_fixed_length(text_ascii(), ndims, unique=True))
    indexes = (draw(int_indexes(n=length)) for length in shape)
    return dict(zip(dims, indexes, strict=True))


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


@composite
def float_data_arrays(
    _draw: DrawFn,
    indexes: MaybeSearchStrategy[Mapping[str, IndexA]] | None = None,
    /,
    *,
    min_value: MaybeSearchStrategy[float | None] = None,
    max_value: MaybeSearchStrategy[float | None] = None,
    allow_nan: MaybeSearchStrategy[bool] = False,
    allow_inf: MaybeSearchStrategy[bool] = False,
    allow_pos_inf: MaybeSearchStrategy[bool] = False,
    allow_neg_inf: MaybeSearchStrategy[bool] = False,
    integral: MaybeSearchStrategy[bool] = False,
    fill: SearchStrategy[Any] | None = None,
    unique: MaybeSearchStrategy[bool] = False,
    name: MaybeSearchStrategy[str | None] = None,
    **indexes_kwargs: MaybeSearchStrategy[IndexA],
) -> DataArrayF:
    """Strategy for generating data arrays of floats."""
    from xarray import DataArray

    draw = lift_draw(_draw)
    indexes_ = draw(_merge_into_dict_of_indexes(indexes, **indexes_kwargs))
    shape = tuple(map(len, indexes_.values()))
    values = draw(
        float_arrays(
            shape=shape,
            min_value=min_value,
            max_value=max_value,
            allow_nan=allow_nan,
            allow_inf=allow_inf,
            allow_pos_inf=allow_pos_inf,
            allow_neg_inf=allow_neg_inf,
            integral=integral,
            fill=fill,
            unique=unique,
        )
    )
    return DataArray(data=values, coords=indexes_, dims=list(indexes_), name=draw(name))


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


@composite
def git_repos(
    _draw: DrawFn, /, *, branch: MaybeSearchStrategy[str | None] = None
) -> Path:
    draw = lift_draw(_draw)
    path = draw(temp_paths())
    with temp_cwd(path):
        _ = run(["git", "init"], check=True)
        _ = run(["git", "config", "user.name", "User"], check=True)
        _ = run(["git", "config", "user.email", "a@z.com"], check=True)
        file = Path(path, "file")
        file.touch()
        file_str = str(file)
        _ = run(["git", "add", file_str], check=True)
        _ = run(["git", "commit", "-m", "add"], check=True)
        _ = run(["git", "rm", file_str], check=True)
        _ = run(["git", "commit", "-m", "rm"], check=True)
        if (branch := draw(branch)) is not None:
            _ = run(["git", "checkout", "-b", branch], check=True)
    return path


@composite
def indexes(
    _draw: DrawFn,
    /,
    *,
    elements: SearchStrategy[Any] | None = None,
    dtype: Any = None,
    n: MaybeSearchStrategy[int] = _INDEX_LENGTHS,
    unique: MaybeSearchStrategy[bool] = True,
    name: MaybeSearchStrategy[Hashable] = None,
    sort: MaybeSearchStrategy[bool] = False,
) -> IndexA:
    """Strategy for generating Indexes."""
    from hypothesis.extra.pandas import indexes as _indexes

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
    index = index.rename(draw(name))
    if draw(sort):
        return index.sort_values()
    return index


def hashables() -> SearchStrategy[Hashable]:
    """Strategy for generating hashable elements."""
    return booleans() | integers() | none() | text_ascii()


@composite
def int_arrays(
    _draw: DrawFn,
    /,
    *,
    shape: MaybeSearchStrategy[Shape] | None = None,
    min_value: MaybeSearchStrategy[int | None] = None,
    max_value: MaybeSearchStrategy[int | None] = None,
    fill: SearchStrategy[Any] | None = None,
    unique: MaybeSearchStrategy[bool] = False,
) -> NDArrayI:
    """Strategy for generating arrays of ints."""
    from hypothesis.extra.numpy import array_shapes, arrays
    from numpy import iinfo, int64

    draw = lift_draw(_draw)
    shape_use = array_shapes() if shape is None else shape
    info = iinfo(int64)
    min_value_, max_value_ = draw(min_value), draw(max_value)
    min_value_use = info.min if min_value_ is None else min_value_
    max_value_use = info.max if max_value_ is None else max_value_
    elements = integers(min_value=min_value_use, max_value=max_value_use)
    strategy: SearchStrategy[NDArrayI] = arrays(
        int64, draw(shape_use), elements=elements, fill=fill, unique=draw(unique)
    )
    return draw(strategy)


@composite
def int_data_arrays(
    _draw: DrawFn,
    indexes: MaybeSearchStrategy[Mapping[str, IndexA]] | None = None,
    /,
    *,
    min_value: MaybeSearchStrategy[int | None] = None,
    max_value: MaybeSearchStrategy[int | None] = None,
    fill: SearchStrategy[Any] | None = None,
    unique: MaybeSearchStrategy[bool] = False,
    name: MaybeSearchStrategy[str | None] = None,
    **indexes_kwargs: MaybeSearchStrategy[IndexA],
) -> DataArrayI:
    """Strategy for generating data arrays of ints."""
    from xarray import DataArray

    draw = lift_draw(_draw)
    indexes_ = draw(_merge_into_dict_of_indexes(indexes, **indexes_kwargs))
    shape = tuple(map(len, indexes_.values()))
    values = draw(
        int_arrays(
            shape=shape,
            min_value=min_value,
            max_value=max_value,
            fill=fill,
            unique=unique,
        )
    )
    return DataArray(data=values, coords=indexes_, dims=list(indexes_), name=draw(name))


def int_indexes(
    *,
    n: MaybeSearchStrategy[int] = _INDEX_LENGTHS,
    unique: MaybeSearchStrategy[bool] = True,
    name: MaybeSearchStrategy[Hashable] = None,
    sort: MaybeSearchStrategy[bool] = False,
) -> SearchStrategy[IndexI]:
    """Strategy for generating integer Indexes."""
    from numpy import int64

    return indexes(
        elements=int64s(), dtype=int64, n=n, unique=unique, name=name, sort=sort
    )


def int32s(
    *,
    min_value: MaybeSearchStrategy[int | None] = None,
    max_value: MaybeSearchStrategy[int | None] = None,
) -> SearchStrategy[int]:
    """Strategy for generating int32s."""
    from numpy import int32

    return _fixed_width_ints(int32, min_value=min_value, max_value=max_value)


def int64s(
    *,
    min_value: MaybeSearchStrategy[int | None] = None,
    max_value: MaybeSearchStrategy[int | None] = None,
) -> SearchStrategy[int]:
    """Strategy for generating int64s."""
    from numpy import int64

    return _fixed_width_ints(int64, min_value=min_value, max_value=max_value)


_MDF = TypeVar("_MDF")


class _MaybeDrawFn(Protocol):
    @overload
    def __call__(self, obj: SearchStrategy[_MDF], /) -> _MDF: ...
    @overload
    def __call__(self, obj: MaybeSearchStrategy[_MDF], /) -> _MDF: ...
    def __call__(self, obj: MaybeSearchStrategy[_MDF], /) -> _MDF:
        raise NotImplementedError(obj)  # pragma: no cover


def lift_draw(draw: DrawFn, /) -> _MaybeDrawFn:
    """Lift the `draw` function to handle non-`SearchStrategy` types."""

    def func(obj: MaybeSearchStrategy[_MDF], /) -> _MDF:
        return draw(obj) if isinstance(obj, SearchStrategy) else obj

    return func


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


@composite
def namespace_mixins(_draw: DrawFn, /) -> type:
    """Strategy for generating task namespace mixins."""
    draw = lift_draw(_draw)
    path = draw(temp_paths())

    class NamespaceMixin:
        task_namespace = path.name

    return NamespaceMixin


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
                case _ as never:  # type: ignore[]
                    assert_never(never)

        @property
        def verbosity(self) -> Verbosity | None:
            match self:
                case Profile.dev | Profile.default | Profile.ci:
                    return Verbosity.normal
                case Profile.debug:
                    return Verbosity.debug
                case _ as never:  # type: ignore[]
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


def settings_with_reduced_examples(
    frac: FloatFinPos = 0.1, /, **kwargs: Any
) -> settings:
    """Set a test to fewer max examples."""
    curr = settings()
    max_examples = max(round(frac * curr.max_examples), 1)
    return settings(max_examples=max_examples, **kwargs)


@composite
def slices(
    _draw: DrawFn,
    iter_len: int,
    /,
    *,
    slice_len: MaybeSearchStrategy[int | None] = None,
) -> slice:
    """Strategy for generating continuous slices from an iterable."""
    draw = lift_draw(_draw)
    if (slice_len_ := draw(slice_len)) is None:
        slice_len_ = draw(integers(0, iter_len))
    elif not 0 <= slice_len_ <= iter_len:
        msg = f"Slice length {slice_len_} exceeds iterable length {iter_len}"
        raise InvalidArgument(msg)
    start = draw(integers(0, iter_len - slice_len_))
    stop = start + slice_len_
    return slice(start, stop)


@composite
def sqlite_engines(
    _draw: DrawFn, /, *, metadata: MetaData | None = None, base: Any = None
) -> Engine:
    """Strategy for generating SQLite engines."""
    from utilities.sqlalchemy import create_engine

    temp_path = _draw(temp_paths())
    path = Path(temp_path, "db.sqlite")
    engine = create_engine("sqlite", database=str(path))
    if metadata is not None:
        metadata.create_all(engine)
    if base is not None:
        base.metadata.create_all(engine)

    # attach temp_path to the engine, so as to keep it alive
    cast(Any, engine).temp_path = temp_path

    return engine


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


@composite
def str_data_arrays(
    _draw: DrawFn,
    indexes: MaybeSearchStrategy[Mapping[str, IndexA]] | None = None,
    /,
    *,
    min_size: MaybeSearchStrategy[int] = 0,
    max_size: MaybeSearchStrategy[int | None] = None,
    allow_none: MaybeSearchStrategy[bool] = False,
    fill: SearchStrategy[Any] | None = None,
    unique: MaybeSearchStrategy[bool] = False,
    name: MaybeSearchStrategy[str | None] = None,
    **indexes_kwargs: MaybeSearchStrategy[IndexA],
) -> DataArrayO:
    """Strategy for generating data arrays of strings."""
    from xarray import DataArray

    draw = lift_draw(_draw)
    indexes_ = draw(_merge_into_dict_of_indexes(indexes, **indexes_kwargs))
    shape = tuple(map(len, indexes_.values()))
    values = draw(
        str_arrays(
            shape=shape,
            min_size=min_size,
            max_size=max_size,
            allow_none=allow_none,
            fill=fill,
            unique=unique,
        )
    )
    return DataArray(data=values, coords=indexes_, dims=list(indexes_), name=draw(name))


@composite
def str_indexes(
    _draw: DrawFn,
    /,
    *,
    min_size: MaybeSearchStrategy[int] = 0,
    max_size: MaybeSearchStrategy[int | None] = None,
    n: MaybeSearchStrategy[int] = _INDEX_LENGTHS,
    unique: MaybeSearchStrategy[bool] = True,
    name: MaybeSearchStrategy[Hashable] = None,
    sort: MaybeSearchStrategy[bool] = False,
) -> IndexS:
    """Strategy for generating string Indexes."""
    from utilities.pandas import string

    draw = lift_draw(_draw)
    elements = text_ascii(min_size=min_size, max_size=max_size)
    index = draw(
        indexes(
            elements=elements, dtype=object, n=n, unique=unique, name=name, sort=sort
        )
    )
    return index.astype(string)


_TEMP_DIR_HYPOTHESIS = Path(TEMP_DIR, "hypothesis")


@composite
def temp_dirs(_draw: DrawFn, /) -> TemporaryDirectory:
    """Search strategy for temporary directories."""
    _TEMP_DIR_HYPOTHESIS.mkdir(exist_ok=True)
    uuid = _draw(uuids())
    return TemporaryDirectory(
        prefix=f"{uuid}__", dir=_TEMP_DIR_HYPOTHESIS, ignore_cleanup_errors=IS_WINDOWS
    )


@composite
def temp_paths(_draw: DrawFn, /) -> Path:
    """Search strategy for paths to temporary directories."""
    temp_dir = _draw(temp_dirs())
    root = temp_dir.path
    cls = type(root)

    class SubPath(cls):
        _temp_dir = temp_dir

    return SubPath(root)


def text_ascii(
    *,
    min_size: MaybeSearchStrategy[int] = 0,
    max_size: MaybeSearchStrategy[int | None] = None,
    disallow_na: MaybeSearchStrategy[bool] = False,
) -> SearchStrategy[str]:
    """Strategy for generating ASCII text."""
    return draw_text(
        characters(whitelist_categories=[], whitelist_characters=ascii_letters),
        min_size=min_size,
        max_size=max_size,
        disallow_na=disallow_na,
    )


def text_clean(
    *,
    min_size: MaybeSearchStrategy[int] = 0,
    max_size: MaybeSearchStrategy[int | None] = None,
    disallow_na: MaybeSearchStrategy[bool] = False,
) -> SearchStrategy[str]:
    """Strategy for generating clean text."""
    return draw_text(
        characters(blacklist_categories=["Z", "C"]),
        min_size=min_size,
        max_size=max_size,
        disallow_na=disallow_na,
    )


def text_printable(
    *,
    min_size: MaybeSearchStrategy[int] = 0,
    max_size: MaybeSearchStrategy[int | None] = None,
    disallow_na: MaybeSearchStrategy[bool] = False,
) -> SearchStrategy[str]:
    """Strategy for generating printable text."""
    return draw_text(
        characters(whitelist_categories=[], whitelist_characters=printable),
        min_size=min_size,
        max_size=max_size,
        disallow_na=disallow_na,
    )


@composite
def timestamps(
    _draw: DrawFn,
    /,
    *,
    min_value: MaybeSearchStrategy[dt.datetime] | None = None,
    max_value: MaybeSearchStrategy[dt.datetime] | None = None,
    allow_nanoseconds: MaybeSearchStrategy[bool] = False,
) -> Timestamp:
    """Strategy for generating Timestamps."""
    from pandas import Timedelta, Timestamp

    from utilities.pandas import TIMESTAMP_MAX_AS_DATETIME, TIMESTAMP_MIN_AS_DATETIME

    draw = lift_draw(_draw)
    min_value_ = TIMESTAMP_MIN_AS_DATETIME if min_value is None else draw(min_value)
    max_value_ = TIMESTAMP_MAX_AS_DATETIME if max_value is None else draw(max_value)
    datetime = draw(datetimes_pd(min_value=min_value_, max_value=max_value_))
    timestamp = cast(Timestamp, Timestamp(datetime))
    if draw(allow_nanoseconds):
        nanoseconds = draw(integers(-999, 999))
        timedelta = Timedelta(nanoseconds=nanoseconds)
        timestamp = cast(Timestamp, timestamp + timedelta)
        _ = assume(min_value_ <= timestamp.floor("us"))
        _ = assume(timestamp.ceil("us") <= max_value_)
    return timestamp


def uint32s(
    *,
    min_value: MaybeSearchStrategy[int | None] = None,
    max_value: MaybeSearchStrategy[int | None] = None,
) -> SearchStrategy[int]:
    """Strategy for generating uint32s."""
    from numpy import uint32

    return _fixed_width_ints(uint32, min_value=min_value, max_value=max_value)


def uint64s(
    *,
    min_value: MaybeSearchStrategy[int | None] = None,
    max_value: MaybeSearchStrategy[int | None] = None,
) -> SearchStrategy[int]:
    """Strategy for generating uint64s."""
    from numpy import uint64

    return _fixed_width_ints(uint64, min_value=min_value, max_value=max_value)


@composite
def versions(
    _draw: DrawFn,
    /,
    *,
    min_version: MaybeSearchStrategy[Version | None] = None,
    max_version: MaybeSearchStrategy[Version | None] = None,
) -> Version:
    """Strategy for generating `Version`s."""
    from semver import Version

    draw = lift_draw(_draw)
    min_version_, max_version_ = (draw(mv) for mv in (min_version, max_version))
    if isinstance(min_version_, Version) and isinstance(max_version_, Version):
        if min_version_ > max_version_:
            msg = f"{min_version_=}, {max_version_=}"
            raise InvalidArgument(msg)
        major = draw(integers(min_version_.major, max_version_.major))
        minor, patch = draw(lists_fixed_length(integers(min_value=0), 2))
        version = Version(major=major, minor=minor, patch=patch)
        _ = assume(min_version_ <= version <= max_version_)
        return version
    if isinstance(min_version_, Version) and (max_version_ is None):
        major = draw(integers(min_value=min_version_.major))
        if major > min_version_.major:
            minor, patch = draw(lists_fixed_length(integers(min_value=0), 2))
        else:
            minor = draw(integers(min_version_.minor))
            if minor > min_version_.minor:
                patch = draw(integers(min_value=0))  # pragma: no cover
            else:
                patch = draw(integers(min_value=min_version_.patch))
    elif (min_version_ is None) and isinstance(max_version_, Version):
        major = draw(integers(0, max_version_.major))
        if major < max_version_.major:
            minor, patch = draw(lists_fixed_length(integers(min_value=0), 2))
        else:
            minor = draw(integers(0, max_version_.minor))
            if minor < max_version_.minor:
                patch = draw(integers(min_value=0))  # pragma: no cover
            else:
                patch = draw(integers(0, max_version_.patch))
    elif (min_version_ is None) and (max_version_ is None):
        major, minor, patch = draw(lists_fixed_length(integers(min_value=0), 3))
    else:
        msg = "Invalid case"  # pragma: no cover
        raise RuntimeError(msg)  # pragma: no cover
    return Version(major=major, minor=minor, patch=patch)


@composite
def _fixed_width_ints(
    _draw: DrawFn,
    dtype: Any,
    /,
    *,
    min_value: MaybeSearchStrategy[int | None] = None,
    max_value: MaybeSearchStrategy[int | None] = None,
) -> int:
    """Strategy for generating int64s."""
    from numpy import iinfo

    draw = lift_draw(_draw)
    min_value_, max_value_ = (draw(mv) for mv in (min_value, max_value))
    info = iinfo(dtype)
    min_value_ = info.min if min_value_ is None else max(info.min, min_value_)
    max_value = info.max if max_value_ is None else min(info.max, max_value_)
    return draw(integers(min_value_, max_value))


@composite
def _merge_into_dict_of_indexes(
    _draw: DrawFn,
    indexes: MaybeSearchStrategy[Mapping[str, IndexA]] | None = None,
    /,
    **indexes_kwargs: MaybeSearchStrategy[IndexA],
) -> dict[str, IndexA]:
    """Merge positional & kwargs of indexes into a dictionary."""
    draw = lift_draw(_draw)
    if (indexes is None) and (len(indexes_kwargs) == 0):
        return draw(dicts_of_indexes())
    indexes_out: dict[str, IndexA] = {}
    if indexes is not None:
        indexes_out |= dict(draw(indexes))
    indexes_out |= {k: draw(v) for k, v in indexes_kwargs.items()}
    return indexes_out


__all__ = [
    "MaybeSearchStrategy",
    "Shape",
    "assume_does_not_raise",
    "bool_arrays",
    "bool_data_arrays",
    "concatenated_arrays",
    "dates_pd",
    "datetimes_pd",
    "datetimes_utc",
    "dicts_of_indexes",
    "draw_text",
    "float_arrays",
    "float_data_arrays",
    "floats_extra",
    "git_repos",
    "hashables",
    "indexes",
    "int32s",
    "int64s",
    "int_arrays",
    "int_data_arrays",
    "int_indexes",
    "lift_draw",
    "lists_fixed_length",
    "namespace_mixins",
    "setup_hypothesis_profiles",
    "slices",
    "sqlite_engines",
    "str_arrays",
    "str_data_arrays",
    "str_indexes",
    "temp_dirs",
    "temp_paths",
    "text_ascii",
    "text_clean",
    "text_printable",
    "timestamps",
    "uint32s",
    "uint64s",
    "versions",
]
