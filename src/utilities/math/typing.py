from __future__ import annotations

from math import isfinite
from typing import Annotated

from utilities.math import (
    is_finite_and_integral,
    is_finite_and_integral_or_nan,
    is_finite_and_negative,
    is_finite_and_negative_or_nan,
    is_finite_and_non_negative,
    is_finite_and_non_negative_or_nan,
    is_finite_and_non_positive,
    is_finite_and_non_positive_or_nan,
    is_finite_and_non_zero,
    is_finite_and_non_zero_or_nan,
    is_finite_and_positive,
    is_finite_and_positive_or_nan,
    is_finite_or_nan,
    is_integral,
    is_integral_or_nan,
    is_negative,
    is_negative_or_nan,
    is_non_negative,
    is_non_negative_or_nan,
    is_non_positive,
    is_non_positive_or_nan,
    is_non_zero,
    is_non_zero_or_nan,
    is_positive,
    is_positive_or_nan,
    is_zero,
    is_zero_or_finite_and_non_micro,
    is_zero_or_finite_and_non_micro_or_nan,
    is_zero_or_nan,
    is_zero_or_non_micro,
    is_zero_or_non_micro_or_nan,
)

# int
IntNeg = Annotated[int, is_negative]
IntNonNeg = Annotated[int, is_non_negative]
IntNonPos = Annotated[int, is_non_positive]
IntNonZr = Annotated[int, is_non_zero]
IntPos = Annotated[int, is_positive]
IntZr = Annotated[int, is_zero]

# float
FloatFin = Annotated[float, isfinite]
FloatFinInt = Annotated[float, is_finite_and_integral]
FloatFinIntNan = Annotated[float, is_finite_and_integral_or_nan]
FloatFinNeg = Annotated[float, is_finite_and_negative]
FloatFinNegNan = Annotated[float, is_finite_and_negative_or_nan]
FloatFinNonNeg = Annotated[float, is_finite_and_non_negative]
FloatFinNonNegNan = Annotated[float, is_finite_and_non_negative_or_nan]
FloatFinNonPos = Annotated[float, is_finite_and_non_positive]
FloatFinNonPosNan = Annotated[float, is_finite_and_non_positive_or_nan]
FloatFinNonZr = Annotated[float, is_finite_and_non_zero]
FloatFinNonZrNan = Annotated[float, is_finite_and_non_zero_or_nan]
FloatFinPos = Annotated[float, is_finite_and_positive]
FloatFinPosNan = Annotated[float, is_finite_and_positive_or_nan]
FloatFinNan = Annotated[float, is_finite_or_nan]
FloatInt = Annotated[float, is_integral]
FloatIntNan = Annotated[float, is_integral_or_nan]
FloatNeg = Annotated[float, is_negative]
FloatNegNan = Annotated[float, is_negative_or_nan]
FloatNonNeg = Annotated[float, is_non_negative]
FloatNonNegNan = Annotated[float, is_non_negative_or_nan]
FloatNonPos = Annotated[float, is_non_positive]
FloatNonPosNan = Annotated[float, is_non_positive_or_nan]
FloatNonZr = Annotated[float, is_non_zero]
FloatNonZrNan = Annotated[float, is_non_zero_or_nan]
FloatPos = Annotated[float, is_positive]
FloatPosNan = Annotated[float, is_positive_or_nan]
FloatZr = Annotated[float, is_zero]
FloatZrFinNonMic = Annotated[float, is_zero_or_finite_and_non_micro]
FloatZrFinNonMicNan = Annotated[float, is_zero_or_finite_and_non_micro_or_nan]
FloatZrNan = Annotated[float, is_zero_or_nan]
FloatZrNonMic = Annotated[float, is_zero_or_non_micro]
FloatZrNonMicNan = Annotated[float, is_zero_or_non_micro_or_nan]
