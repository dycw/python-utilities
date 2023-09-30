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

from utilities.math.typing import FloatFin
from utilities.math.typing import FloatFinInt
from utilities.math.typing import FloatFinIntNan
from utilities.math.typing import FloatFinNan
from utilities.math.typing import FloatFinNeg
from utilities.math.typing import FloatFinNegNan
from utilities.math.typing import FloatFinNonNeg
from utilities.math.typing import FloatFinNonNegNan
from utilities.math.typing import FloatFinNonPos
from utilities.math.typing import FloatFinNonPosNan
from utilities.math.typing import FloatFinNonZr
from utilities.math.typing import FloatFinNonZrNan
from utilities.math.typing import FloatFinPos
from utilities.math.typing import FloatFinPosNan
from utilities.math.typing import FloatInt
from utilities.math.typing import FloatIntNan
from utilities.math.typing import FloatNeg
from utilities.math.typing import FloatNegNan
from utilities.math.typing import FloatNonNeg
from utilities.math.typing import FloatNonNegNan
from utilities.math.typing import FloatNonPos
from utilities.math.typing import FloatNonPosNan
from utilities.math.typing import FloatNonZr
from utilities.math.typing import FloatNonZrNan
from utilities.math.typing import FloatPos
from utilities.math.typing import FloatPosNan
from utilities.math.typing import FloatZr
from utilities.math.typing import FloatZrFinNonMic
from utilities.math.typing import FloatZrFinNonMicNan
from utilities.math.typing import FloatZrNan
from utilities.math.typing import FloatZrNonMic
from utilities.math.typing import FloatZrNonMicNan
from utilities.math.typing import IntNeg
from utilities.math.typing import IntNonNeg
from utilities.math.typing import IntNonPos
from utilities.math.typing import IntNonZr
from utilities.math.typing import IntPos
from utilities.math.typing import IntZr


class TestHints:
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
    def test_checks(self, x: float, hint: Any) -> None:
        with suppress(BeartypeDoorHintViolation):
            die_if_unbearable(x, hint)
