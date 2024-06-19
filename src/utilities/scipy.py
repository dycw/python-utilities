from __future__ import annotations

from typing import TYPE_CHECKING

from numpy import apply_along_axis, clip, full_like, isnan, nan, zeros_like
from scipy.stats import norm

from utilities.numpy import NDArrayF, NDArrayF1, is_zero

if TYPE_CHECKING:
    from utilities.math import FloatFinNonNeg


def ppf(array: NDArrayF, cutoff: FloatFinNonNeg, /, *, axis: int = -1) -> NDArrayF:
    """Apply the PPF transform to an array of data."""
    return apply_along_axis(_ppf_1d, axis, array, cutoff)


def _ppf_1d(array: NDArrayF1, cutoff: FloatFinNonNeg, /) -> NDArrayF1:
    if (i := isnan(array)).all():
        return array
    if i.any():
        j = ~i
        out = full_like(array, nan, dtype=float)
        out[j] = _ppf_1d(array[j], cutoff)
        return out
    low, high = min(array), max(array)
    if is_zero(span := high - low):
        return zeros_like(array, dtype=float)
    centred = (array - low) / span
    phi = norm.cdf(-cutoff)
    ppf = norm.ppf((1.0 - 2.0 * phi) * centred + phi)
    return clip(ppf, a_min=-cutoff, a_max=cutoff)


__all__ = ["ppf"]
