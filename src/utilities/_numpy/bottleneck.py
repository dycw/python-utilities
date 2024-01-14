from __future__ import annotations

from dataclasses import dataclass
from typing import cast

from bottleneck import push
from numpy import errstate, flip, isfinite, nan, where
from typing_extensions import override

from utilities._numpy.common import NDArrayF, NDArrayI, shift


def ffill(array: NDArrayF, /, *, limit: int | None = None, axis: int = -1) -> NDArrayF:
    """Forward fill the elements in an array."""
    return push(array, n=limit, axis=axis)


def pct_change(
    array: NDArrayF | NDArrayI,
    /,
    *,
    limit: int | None = None,
    n: int = 1,
    axis: int = -1,
) -> NDArrayF:
    """Compute the percentage change in an array."""
    if n == 0:
        raise PctChangeError
    if n > 0:
        filled = ffill(array.astype(float), limit=limit, axis=axis)
        shifted = shift(filled, n=n, axis=axis)
        with errstate(all="ignore"):
            ratio = (filled / shifted) if n >= 0 else (shifted / filled)
        return where(isfinite(array), ratio - 1.0, nan)
    flipped = cast(NDArrayF | NDArrayI, flip(array, axis=axis))
    result = pct_change(flipped, limit=limit, n=-n, axis=axis)
    return flip(result, axis=axis)


@dataclass(frozen=True, kw_only=True, slots=True)
class PctChangeError(Exception):
    @override
    def __str__(self) -> str:
        return "Shift must be non-zero"


__all__ = ["PctChangeError", "ffill", "pct_change"]
