from contextlib import suppress
from typing import Any, Union

from beartype.door import die_if_unbearable
from beartype.roar import BeartypeDoorHintViolation
from hypothesis import Phase, given, settings
from hypothesis.strategies import floats, integers
from pytest import mark, param

from utilities.math.typing import (
    FloatFin,
    FloatFinInt,
    FloatFinIntNan,
    FloatFinNan,
    FloatFinNeg,
    FloatFinNegNan,
    FloatFinNonNeg,
    FloatFinNonNegNan,
    FloatFinNonPos,
    FloatFinNonPosNan,
    FloatFinNonZr,
    FloatFinNonZrNan,
    FloatFinPos,
    FloatFinPosNan,
    FloatInt,
    FloatIntNan,
    FloatNeg,
    FloatNegNan,
    FloatNonNeg,
    FloatNonNegNan,
    FloatNonPos,
    FloatNonPosNan,
    FloatNonZr,
    FloatNonZrNan,
    FloatPos,
    FloatPosNan,
    FloatZr,
    FloatZrFinNonMic,
    FloatZrFinNonMicNan,
    FloatZrNan,
    FloatZrNonMic,
    FloatZrNonMicNan,
    IntNeg,
    IntNonNeg,
    IntNonPos,
    IntNonZr,
    IntPos,
    IntZr,
)


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
    def test_checks(self, x: Union[int, float], hint: Any) -> None:
        with suppress(BeartypeDoorHintViolation):
            die_if_unbearable(x, hint)
