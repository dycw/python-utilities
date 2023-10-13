from __future__ import annotations

from numpy import apply_along_axis
from numpy import clip
from numpy import full_like
from numpy import isnan
from numpy import nan
from numpy import zeros_like
from scipy.stats import norm

from utilities.math import FloatFinNonNeg
from utilities.numpy import NDArrayF
from utilities.numpy import NDArrayF1
from utilities.numpy import is_zero


def ppf(
    array: NDArrayF, cutoff: FloatFinNonNeg, /, *, axis: int = -1
) -> NDArrayF:
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
