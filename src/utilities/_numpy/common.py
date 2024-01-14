from __future__ import annotations

from dataclasses import dataclass
from itertools import repeat
from typing import Any

from numpy import bool_, float64, int64, nan, object_, roll
from numpy.typing import NDArray
from typing_extensions import override

# annotations - dtypes


NDArrayA = NDArray[Any]
NDArrayB = NDArray[bool_]
NDArrayF = NDArray[float64]
NDArrayI = NDArray[int64]
NDArrayO = NDArray[object_]


# shift


def shift(array: NDArrayF | NDArrayI, /, *, n: int = 1, axis: int = -1) -> NDArrayF:
    """Shift the elements of an array."""
    if n == 0:
        raise ShiftError
    as_float = array.astype(float)
    shifted = roll(as_float, n, axis=axis)
    indexer = list(repeat(slice(None), times=array.ndim))
    indexer[axis] = slice(n) if n >= 0 else slice(n, None)
    shifted[tuple(indexer)] = nan
    return shifted


@dataclass(frozen=True, kw_only=True, slots=True)
class ShiftError(Exception):
    @override
    def __str__(self) -> str:
        return "Shift must be non-zero"


__all__ = [
    "NDArrayA",
    "NDArrayB",
    "NDArrayF",
    "NDArrayI",
    "NDArrayO",
    "ShiftError",
    "shift",
]
