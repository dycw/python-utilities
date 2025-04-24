from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal, cast, overload

import statsmodels.tsa.stattools

if TYPE_CHECKING:
    from utilities.numpy import NDArrayF


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


__all__ = ["ACFMissing", "acf"]
