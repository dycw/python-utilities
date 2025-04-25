from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal, cast, overload

import numpy as np
import statsmodels.tsa.stattools
from numpy import arange, argmax, interp, nan

from utilities.numpy import shift

if TYPE_CHECKING:
    from utilities.numpy import NDArrayF


def ac_halflife(
    array: NDArrayF,
    /,
    *,
    adjusted: bool = False,
    fft: bool = True,
    bartlett_confint: bool = True,
    missing: ACFMissing = "none",
    step: float = 0.01,
) -> float:
    """Compute the autocorrelation halflife."""
    (n,) = array.shape
    acfs = acf(
        array,
        adjusted=adjusted,
        nlags=n,
        fft=fft,
        bartlett_confint=bartlett_confint,
        missing=missing,
    )
    lags = arange(0, n, step=step)
    interp_acfs = interp(lags, arange(n), acfs)
    is_half = (shift(interp_acfs) > 0.5) & (interp_acfs <= 0.5)
    return lags[argmax(is_half)].item() if np.any(is_half) else nan


##


type ACFMissing = Literal["none", "raise", "conservative", "drop"]


@overload
def acf(
    array: NDArrayF,
    /,
    *,
    adjusted: bool = False,
    nlags: int | None = None,
    qstat: Literal[False] = False,
    fft: bool = True,
    alpha: None = None,
    bartlett_confint: bool = True,
    missing: ACFMissing = "none",
) -> NDArrayF: ...
@overload
def acf(
    array: NDArrayF,
    /,
    *,
    adjusted: bool = False,
    nlags: int | None = None,
    qstat: Literal[False] = False,
    fft: bool = True,
    alpha: float,
    bartlett_confint: bool = True,
    missing: ACFMissing = "none",
) -> tuple[NDArrayF, NDArrayF]: ...
@overload
def acf(
    array: NDArrayF,
    /,
    *,
    adjusted: bool = False,
    nlags: int | None = None,
    qstat: Literal[True],
    fft: bool = True,
    alpha: float,
    bartlett_confint: bool = True,
    missing: ACFMissing = "none",
) -> tuple[NDArrayF, NDArrayF, NDArrayF, NDArrayF]: ...
@overload
def acf(
    array: NDArrayF,
    /,
    *,
    adjusted: bool = False,
    nlags: int | None = None,
    qstat: Literal[True],
    fft: bool = True,
    alpha: None = None,
    bartlett_confint: bool = True,
    missing: ACFMissing = "none",
) -> tuple[NDArrayF, NDArrayF, NDArrayF]: ...
@overload
def acf(
    array: NDArrayF,
    /,
    *,
    adjusted: bool = False,
    nlags: int | None = None,
    qstat: bool = False,
    fft: bool = True,
    alpha: float | None = None,
    bartlett_confint: bool = True,
    missing: ACFMissing = "none",
) -> (
    NDArrayF
    | tuple[NDArrayF, NDArrayF]
    | tuple[NDArrayF, NDArrayF, NDArrayF]
    | tuple[NDArrayF, NDArrayF, NDArrayF, NDArrayF]
): ...
def acf(
    array: NDArrayF,
    /,
    *,
    adjusted: bool = False,
    nlags: int | None = None,
    qstat: bool = False,
    fft: bool = True,
    alpha: float | None = None,
    bartlett_confint: bool = True,
    missing: ACFMissing = "none",
) -> (
    NDArrayF
    | tuple[NDArrayF, NDArrayF]
    | tuple[NDArrayF, NDArrayF, NDArrayF]
    | tuple[NDArrayF, NDArrayF, NDArrayF, NDArrayF]
):
    """Typed version of `acf`."""
    return cast(
        "Any",
        statsmodels.tsa.stattools.acf(
            array,
            adjusted=adjusted,
            nlags=nlags,
            qstat=qstat,
            fft=fft,
            alpha=alpha,
            bartlett_confint=bartlett_confint,
            missing=missing,
        ),
    )


__all__ = ["ACFMissing", "ac_halflife", "acf"]
