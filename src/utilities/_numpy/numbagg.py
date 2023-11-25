from __future__ import annotations

from typing import Any, cast

from numbagg import move_exp_nanmean, move_exp_nansum
from numpy import exp, log

from utilities._numpy.common import NDArrayF
from utilities.math import FloatFinPos


def ewma(array: NDArrayF, halflife: FloatFinPos, /, *, axis: int = -1) -> NDArrayF:
    """Compute the EWMA of an array."""
    alpha = _exp_weighted_alpha(halflife)
    return cast(Any, move_exp_nanmean)(array, axis=axis, alpha=alpha)


def exp_moving_sum(
    array: NDArrayF, halflife: FloatFinPos, /, *, axis: int = -1
) -> NDArrayF:
    """Compute the exponentially-weighted moving sum of an array."""
    alpha = _exp_weighted_alpha(halflife)
    return cast(Any, move_exp_nansum)(array, axis=axis, alpha=alpha)


def _exp_weighted_alpha(halflife: FloatFinPos, /) -> float:
    """Get the alpha."""
    decay = 1.0 - exp(log(0.5) / halflife)
    com = 1.0 / decay - 1.0
    return 1.0 / (1.0 + com)


__all__ = ["ewma", "exp_moving_sum"]
