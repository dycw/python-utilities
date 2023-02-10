import builtins
from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from math import ceil, floor, inf, isfinite, nan
from os import getenv
from pathlib import Path
from re import search
from string import ascii_letters, printable
from typing import Any, Optional, Protocol, TypeVar, cast, overload

from beartype import beartype
from hypothesis import HealthCheck, Verbosity, assume, settings
from hypothesis.errors import InvalidArgument
from hypothesis.strategies import (
    DrawFn,
    SearchStrategy,
    characters,
    composite,
    floats,
    integers,
    just,
    lists,
    sampled_from,
    text,
    uuids,
)

from utilities.hypothesis.typing import MaybeSearchStrategy
from utilities.tempfile import TEMP_DIR, TemporaryDirectory
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


@beartype
def setup_hypothesis_profiles() -> None:
    """Set up the hypothesis profiles."""
    for name, max_examples, kwargs in [
        ("dev", 10, {}),
        ("default", 100, {}),
        ("ci", 1000, {}),
        ("debug", 10, {"verbosity": Verbosity.verbose}),
    ]:
        settings.register_profile(
            name,
            max_examples=max_examples,
            report_multiple_bugs=True,
            suppress_health_check={HealthCheck.filter_too_much},
            deadline=None,
            print_blob=True,
            **kwargs,
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
) -> SearchStrategy[str]:
    """Strategy for generating ASCII text."""
    return _draw_text(
        characters(whitelist_categories=[], whitelist_characters=ascii_letters),
        min_size=min_size,
        max_size=max_size,
    )


@beartype
def text_clean(
    *,
    min_size: MaybeSearchStrategy[int] = 0,
    max_size: MaybeSearchStrategy[Optional[int]] = None,
) -> SearchStrategy[str]:
    """Strategy for generating clean text."""
    return _draw_text(
        characters(blacklist_categories=["Z", "C"]),
        min_size=min_size,
        max_size=max_size,
    )


@beartype
def text_printable(
    *,
    min_size: MaybeSearchStrategy[int] = 0,
    max_size: MaybeSearchStrategy[Optional[int]] = None,
) -> SearchStrategy[str]:
    """Strategy for generating printable text."""
    return _draw_text(
        characters(whitelist_categories=[], whitelist_characters=printable),
        min_size=min_size,
        max_size=max_size,
    )


@composite
def _draw_text(
    _draw: DrawFn,
    alphabet: MaybeSearchStrategy[str],
    /,
    *,
    min_size: MaybeSearchStrategy[int] = 0,
    max_size: MaybeSearchStrategy[Optional[int]] = None,
) -> str:
    draw = lift_draw(_draw)
    return draw(
        text(alphabet, min_size=draw(min_size), max_size=draw(max_size)),
    )
