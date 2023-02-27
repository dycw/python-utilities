import builtins
import datetime as dt
from collections.abc import Iterable
from collections.abc import Iterator
from contextlib import contextmanager
from math import ceil
from math import floor
from math import inf
from math import isfinite
from math import nan
from os import environ
from os import getenv
from pathlib import Path
from re import search
from string import ascii_letters
from string import printable
from typing import Any
from typing import Optional
from typing import Protocol
from typing import TypeVar
from typing import cast
from typing import overload

from beartype import beartype
from hypothesis import HealthCheck
from hypothesis import Phase
from hypothesis import Verbosity
from hypothesis import assume
from hypothesis import settings
from hypothesis.errors import InvalidArgument
from hypothesis.strategies import DrawFn
from hypothesis.strategies import SearchStrategy
from hypothesis.strategies import characters
from hypothesis.strategies import composite
from hypothesis.strategies import datetimes
from hypothesis.strategies import floats
from hypothesis.strategies import integers
from hypothesis.strategies import just
from hypothesis.strategies import lists
from hypothesis.strategies import sampled_from
from hypothesis.strategies import text
from hypothesis.strategies import uuids

from utilities.datetime import UTC
from utilities.hypothesis.typing import MaybeSearchStrategy
from utilities.pandas import TIMESTAMP_MAX_AS_DATETIME
from utilities.pandas import TIMESTAMP_MIN_AS_DATETIME
from utilities.tempfile import TEMP_DIR
from utilities.tempfile import TemporaryDirectory
from utilities.text import ensure_str


@contextmanager
@beartype
def assume_does_not_raise(
    *exceptions: type[Exception],
    match: Optional[str] = None,
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
@beartype
def datetimes_utc(
    _draw: Any,
    /,
    *,
    min_value: MaybeSearchStrategy[dt.datetime] = TIMESTAMP_MIN_AS_DATETIME,
    max_value: MaybeSearchStrategy[dt.datetime] = TIMESTAMP_MAX_AS_DATETIME,
) -> dt.datetime:
    """Strategy for generating datetimes with the UTC timezone."""
    draw = lift_draw(_draw)
    return draw(
        datetimes(
            min_value=draw(min_value).replace(tzinfo=None),
            max_value=draw(max_value).replace(tzinfo=None),
            timezones=just(UTC),
        ),
    )


@composite
@beartype
def floats_extra(
    _draw: Any,
    /,
    *,
    min_value: MaybeSearchStrategy[Optional[float]] = None,
    max_value: MaybeSearchStrategy[Optional[float]] = None,
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


_MDF = TypeVar("_MDF")


class _MaybeDrawFn(Protocol):
    @overload
    def __call__(self, obj: SearchStrategy[_MDF], /) -> _MDF:
        ...

    @overload
    def __call__(self, obj: MaybeSearchStrategy[_MDF], /) -> _MDF:
        ...

    def __call__(self, obj: MaybeSearchStrategy[_MDF], /) -> _MDF:
        raise NotImplementedError(obj)  # pragma: no cover


def lift_draw(draw: DrawFn, /) -> _MaybeDrawFn:
    """Lift the `draw` function to handle non-`SearchStrategy` types."""

    @beartype
    def func(obj: MaybeSearchStrategy[_MDF], /) -> _MDF:
        return draw(obj) if isinstance(obj, SearchStrategy) else obj

    return func


_TLFL = TypeVar("_TLFL")


@composite
@beartype
def lists_fixed_length(
    _draw: Any,
    strategy: SearchStrategy[_TLFL],
    size: MaybeSearchStrategy[int],
    /,
    *,
    unique: MaybeSearchStrategy[bool] = False,
    sorted: MaybeSearchStrategy[bool] = False,  # noqa: A002
) -> list[_TLFL]:
    """Strategy for generating lists of a fixed length."""
    draw = lift_draw(_draw)
    size_ = draw(size)
    elements = draw(
        lists(strategy, min_size=size_, max_size=size_, unique=draw(unique)),
    )
    if draw(sorted):
        return builtins.sorted(cast(Iterable[Any], elements))
    return elements


_MAX_EXAMPLES: str = "MAX_EXAMPLES"
_NO_SHRINK: str = "NO_SHRINK"


@beartype
def setup_hypothesis_profiles(
    *,
    max_examples: str = _MAX_EXAMPLES,
    no_shrink: str = _NO_SHRINK,
    suppress_health_check: Iterable[HealthCheck] = (),
) -> None:
    """Set up the hypothesis profiles."""

    @beartype
    def yield_phases() -> Iterator[Phase]:
        yield Phase.explicit
        yield Phase.reuse
        yield Phase.generate
        yield Phase.target
        if not bool(getenv(no_shrink, default=False)):
            yield Phase.shrink

    phases = set(yield_phases())
    for name, default_max_examples, verbosity in [
        ("dev", 10, None),
        ("default", 100, None),
        ("ci", 1000, None),
        ("debug", 10, Verbosity.verbose),
    ]:
        try:
            env_var = environ[max_examples]
        except KeyError:
            max_examples_use = default_max_examples
        else:
            max_examples_use = int(env_var)
        verbosity_use = {} if verbosity is None else {"verbosity": verbosity}
        settings.register_profile(
            name,
            max_examples=max_examples_use,
            **verbosity_use,
            phases=phases,
            report_multiple_bugs=True,
            suppress_health_check=suppress_health_check,
            deadline=None,
            print_blob=True,
        )
    settings.load_profile(getenv("HYPOTHESIS_PROFILE", "default"))


@composite
@beartype
def slices(
    _draw: Any,
    iter_len: int,
    /,
    *,
    slice_len: MaybeSearchStrategy[Optional[int]] = None,
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
@beartype
def temp_dirs(_draw: Any, /) -> TemporaryDirectory:
    """Search strategy for temporary directories."""
    dir_ = TEMP_DIR.joinpath("hypothesis")
    dir_.mkdir(exist_ok=True)
    uuid = _draw(uuids())
    return TemporaryDirectory(prefix=f"{uuid}__", dir=dir_.as_posix())


@composite
@beartype
def temp_paths(_draw: Any, /) -> Path:
    """Search strategy for paths to temporary directories."""
    temp_dir = _draw(temp_dirs())
    root = temp_dir.name
    cls = type(root)

    class SubPath(cls):
        _temp_dir = temp_dir

    return SubPath(root)


@beartype
def text_ascii(
    *,
    min_size: MaybeSearchStrategy[int] = 0,
    max_size: MaybeSearchStrategy[Optional[int]] = None,
    disallow_na: MaybeSearchStrategy[bool] = False,
) -> SearchStrategy[str]:
    """Strategy for generating ASCII text."""
    return _draw_text(
        characters(whitelist_categories=[], whitelist_characters=ascii_letters),
        min_size=min_size,
        max_size=max_size,
        disallow_na=disallow_na,
    )


@beartype
def text_clean(
    *,
    min_size: MaybeSearchStrategy[int] = 0,
    max_size: MaybeSearchStrategy[Optional[int]] = None,
    disallow_na: MaybeSearchStrategy[bool] = False,
) -> SearchStrategy[str]:
    """Strategy for generating clean text."""
    return _draw_text(
        characters(blacklist_categories=["Z", "C"]),
        min_size=min_size,
        max_size=max_size,
        disallow_na=disallow_na,
    )


@beartype
def text_printable(
    *,
    min_size: MaybeSearchStrategy[int] = 0,
    max_size: MaybeSearchStrategy[Optional[int]] = None,
    disallow_na: MaybeSearchStrategy[bool] = False,
) -> SearchStrategy[str]:
    """Strategy for generating printable text."""
    return _draw_text(
        characters(whitelist_categories=[], whitelist_characters=printable),
        min_size=min_size,
        max_size=max_size,
        disallow_na=disallow_na,
    )


@composite
def _draw_text(
    _draw: DrawFn,
    alphabet: MaybeSearchStrategy[str],
    /,
    *,
    min_size: MaybeSearchStrategy[int] = 0,
    max_size: MaybeSearchStrategy[Optional[int]] = None,
    disallow_na: MaybeSearchStrategy[bool] = False,
) -> str:
    draw = lift_draw(_draw)
    drawn = draw(
        text(alphabet, min_size=draw(min_size), max_size=draw(max_size)),
    )
    if draw(disallow_na):
        _ = assume(drawn != "NA")
    return drawn
