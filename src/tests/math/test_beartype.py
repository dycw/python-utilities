from __future__ import annotations

from contextlib import suppress
from typing import Any

from beartype.door import die_if_unbearable
from beartype.roar import BeartypeDoorHintViolation
from hypothesis import Phase
from hypothesis import given
from hypothesis import settings
from hypothesis.strategies import floats
from hypothesis.strategies import integers
from pytest import mark
from pytest import param

from utilities.math import FloatFin
from utilities.math import FloatFinInt
from utilities.math import FloatFinIntNan
from utilities.math import FloatFinNan
from utilities.math import FloatFinNeg
from utilities.math import FloatFinNegNan
from utilities.math import FloatFinNonNeg
from utilities.math import FloatFinNonNegNan
from utilities.math import FloatFinNonPos
from utilities.math import FloatFinNonPosNan
from utilities.math import FloatFinNonZr
from utilities.math import FloatFinNonZrNan
from utilities.math import FloatFinPos
from utilities.math import FloatFinPosNan
from utilities.math import FloatInt
from utilities.math import FloatIntNan
from utilities.math import FloatNeg
from utilities.math import FloatNegNan
from utilities.math import FloatNonNeg
from utilities.math import FloatNonNegNan
from utilities.math import FloatNonPos
from utilities.math import FloatNonPosNan
from utilities.math import FloatNonZr
from utilities.math import FloatNonZrNan
from utilities.math import FloatPos
from utilities.math import FloatPosNan
from utilities.math import FloatZr
from utilities.math import FloatZrFinNonMic
from utilities.math import FloatZrFinNonMicNan
from utilities.math import FloatZrNan
from utilities.math import FloatZrNonMic
from utilities.math import FloatZrNonMicNan
from utilities.math import IntNeg
from utilities.math import IntNonNeg
from utilities.math import IntNonPos
from utilities.math import IntNonZr
from utilities.math import IntPos
from utilities.math import IntZr


class TestAnnotations:
    @given(x=integers() | floats(allow_infinity=True, allow_nan=True))
    @mark.parametrize(
        "hint",
        [
            param(IntNeg),
            param(IntNonNeg),
            param(IntNonPos),
            param(IntNonZr),
            param(IntPos),
            param(IntZr),
            param(FloatFin),
            param(FloatFinInt),
            param(FloatFinIntNan),
            param(FloatFinNeg),
            param(FloatFinNegNan),
            param(FloatFinNonNeg),
            param(FloatFinNonNegNan),
            param(FloatFinNonPos),
            param(FloatFinNonPosNan),
            param(FloatFinNonZr),
            param(FloatFinNonZrNan),
            param(FloatFinPos),
            param(FloatFinPosNan),
            param(FloatFinNan),
            param(FloatInt),
            param(FloatIntNan),
            param(FloatNeg),
            param(FloatNegNan),
            param(FloatNonNeg),
            param(FloatNonNegNan),
            param(FloatNonPos),
            param(FloatNonPosNan),
            param(FloatNonZr),
            param(FloatNonZrNan),
            param(FloatPos),
            param(FloatPosNan),
            param(FloatZr),
            param(FloatZrFinNonMic),
            param(FloatZrFinNonMicNan),
            param(FloatZrNan),
            param(FloatZrNonMic),
            param(FloatZrNonMicNan),
        ],
    )
    @settings(max_examples=1, phases={Phase.generate})
    def test_main(self, *, x: float, hint: Any) -> None:
        with suppress(BeartypeDoorHintViolation):
            die_if_unbearable(x, hint)
