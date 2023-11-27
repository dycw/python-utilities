from __future__ import annotations

import builtins
from collections.abc import Iterable
from math import ceil, floor, inf, isfinite, nan
from pathlib import Path
from string import ascii_letters
from typing import Any, Protocol, TypeVar, cast, overload

from hypothesis import assume
from hypothesis.strategies import (
    DrawFn,
    SearchStrategy,
    characters,
    composite,
    floats,
    just,
    lists,
    sampled_from,
    text,
    uuids,
)

from utilities.pathvalidate import valid_path
from utilities.platform import IS_WINDOWS
from utilities.tempfile import TEMP_DIR, TemporaryDirectory

_T = TypeVar("_T")
MaybeSearchStrategy = _T | SearchStrategy[_T]
Shape = int | tuple[int, ...]


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


_TEMP_DIR_HYPOTHESIS = valid_path(TEMP_DIR, "hypothesis")


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


__all__ = [
    "MaybeSearchStrategy",
    "Shape",
    "draw_text",
    "floats_extra",
    "lift_draw",
    "lists_fixed_length",
    "temp_dirs",
    "temp_paths",
    "text_ascii",
]
