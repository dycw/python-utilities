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
from numpy import inf
from numpy import nan
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
from utilities.math import is_at_least
from utilities.math import is_at_least_or_nan
from utilities.math import is_at_most
from utilities.math import is_at_most_or_nan
from utilities.math import is_between
from utilities.math import is_between_or_nan
from utilities.math import is_finite_and_integral
from utilities.math import is_finite_and_integral_or_nan
from utilities.math import is_finite_and_negative
from utilities.math import is_finite_and_negative_or_nan
from utilities.math import is_finite_and_non_negative
from utilities.math import is_finite_and_non_negative_or_nan
from utilities.math import is_finite_and_non_positive
from utilities.math import is_finite_and_non_positive_or_nan
from utilities.math import is_finite_and_non_zero
from utilities.math import is_finite_and_non_zero_or_nan
from utilities.math import is_finite_and_positive
from utilities.math import is_finite_and_positive_or_nan
from utilities.math import is_finite_or_nan
from utilities.math import is_greater_than
from utilities.math import is_greater_than_or_nan
from utilities.math import is_integral
from utilities.math import is_integral_or_nan
from utilities.math import is_less_than
from utilities.math import is_less_than_or_nan
from utilities.math import is_negative
from utilities.math import is_negative_or_nan
from utilities.math import is_non_negative
from utilities.math import is_non_negative_or_nan
from utilities.math import is_non_positive
from utilities.math import is_non_positive_or_nan
from utilities.math import is_non_zero
from utilities.math import is_non_zero_or_nan
from utilities.math import is_positive
from utilities.math import is_positive_or_nan
from utilities.math import is_zero
from utilities.math import is_zero_or_finite_and_non_micro
from utilities.math import is_zero_or_finite_and_non_micro_or_nan
from utilities.math import is_zero_or_nan
from utilities.math import is_zero_or_non_micro
from utilities.math import is_zero_or_non_micro_or_nan


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
    def test_checks(self, *, x: float, hint: Any) -> None:
        with suppress(BeartypeDoorHintViolation):
            die_if_unbearable(x, hint)


class TestChecks:
    @mark.parametrize(
        ("x", "y", "expected"),
        [
            param(0.0, -inf, True),
            param(0.0, -1.0, True),
            param(0.0, -1e-6, True),
            param(0.0, -1e-7, True),
            param(0.0, -1e-8, True),
            param(0.0, 0.0, True),
            param(0.0, 1e-8, True),
            param(0.0, 1e-7, False),
            param(0.0, 1e-6, False),
            param(0.0, 1.0, False),
            param(0.0, inf, False),
            param(0.0, nan, False),
        ],
    )
    def test_is_at_least(self, *, x: float, y: float, expected: bool) -> None:
        assert is_at_least(x, y, abs_tol=1e-8) is expected

    @mark.parametrize(
        "y",
        [
            param(-inf),
            param(-1.0),
            param(0.0),
            param(1.0),
            param(inf),
            param(nan),
        ],
    )
    def test_is_at_least_or_nan(self, *, y: float) -> None:
        assert is_at_least_or_nan(nan, y)

    @mark.parametrize(
        ("x", "y", "expected"),
        [
            param(0.0, -inf, False),
            param(0.0, -1.0, False),
            param(0.0, -1e-6, False),
            param(0.0, -1e-7, False),
            param(0.0, -1e-8, True),
            param(0.0, 0.0, True),
            param(0.0, 1e-8, True),
            param(0.0, 1e-7, True),
            param(0.0, 1e-6, True),
            param(0.0, 1.0, True),
            param(0.0, inf, True),
            param(0.0, nan, False),
        ],
    )
    def test_is_at_most(self, *, x: float, y: float, expected: bool) -> None:
        assert is_at_most(x, y, abs_tol=1e-8) is expected

    @mark.parametrize(
        "y",
        [
            param(-inf),
            param(-1.0),
            param(0.0),
            param(1.0),
            param(inf),
            param(nan),
        ],
    )
    def test_is_at_most_or_nan(self, *, y: float) -> None:
        assert is_at_most_or_nan(nan, y)

    @mark.parametrize(
        ("x", "low", "high", "expected"),
        [
            param(0.0, -1.0, -1.0, False),
            param(0.0, -1.0, 0.0, True),
            param(0.0, -1.0, 1.0, True),
            param(0.0, 0.0, -1.0, False),
            param(0.0, 0.0, 0.0, True),
            param(0.0, 0.0, 1.0, True),
            param(0.0, 1.0, -1.0, False),
            param(0.0, 1.0, 0.0, False),
            param(0.0, 1.0, 1.0, False),
            param(nan, -1.0, 1.0, False),
        ],
    )
    def test_is_between(
        self, *, x: float, low: float, high: float, expected: bool
    ) -> None:
        assert is_between(x, low, high, abs_tol=1e-8) is expected

    @mark.parametrize(
        "low",
        [
            param(-inf),
            param(-1.0),
            param(0.0),
            param(1.0),
            param(inf),
            param(nan),
        ],
    )
    @mark.parametrize(
        "high",
        [
            param(-inf),
            param(-1.0),
            param(0.0),
            param(1.0),
            param(inf),
            param(nan),
        ],
    )
    def test_is_between_or_nan(self, *, low: float, high: float) -> None:
        assert is_between_or_nan(nan, low, high)

    @mark.parametrize(
        ("x", "expected"),
        [
            param(-inf, False),
            param(-2.0, True),
            param(-1.5, False),
            param(-1.0, True),
            param(-0.5, False),
            param(-1e-6, False),
            param(-1e-7, False),
            param(-1e-8, True),
            param(0.0, True),
            param(1e-8, True),
            param(1e-7, False),
            param(1e-6, False),
            param(0.5, False),
            param(1.0, True),
            param(1.5, False),
            param(2.0, True),
            param(inf, False),
            param(nan, False),
        ],
    )
    def test_is_finite_and_integral(self, *, x: float, expected: bool) -> None:
        assert is_finite_and_integral(x, abs_tol=1e-8) is expected

    def test_is_finite_and_integral_or_nan(self) -> None:
        assert is_finite_and_integral_or_nan(nan)

    @mark.parametrize(
        ("x", "expected"),
        [
            param(-inf, False),
            param(-1.0, True),
            param(-1e-6, True),
            param(-1e-7, True),
            param(-1e-8, False),
            param(0.0, False),
            param(1e-8, False),
            param(1e-7, False),
            param(1e-6, False),
            param(1.0, False),
            param(inf, False),
            param(nan, False),
        ],
    )
    def test_is_finite_and_negative(self, *, x: float, expected: bool) -> None:
        assert is_finite_and_negative(x, abs_tol=1e-8) is expected

    def test_is_finite_and_negative_or_nan(self) -> None:
        assert is_finite_and_negative_or_nan(nan)

    @mark.parametrize(
        ("x", "expected"),
        [
            param(-inf, False),
            param(-1.0, False),
            param(-1e-6, False),
            param(-1e-7, False),
            param(-1e-8, True),
            param(0.0, True),
            param(1e-8, True),
            param(1e-7, True),
            param(1e-6, True),
            param(1.0, True),
            param(inf, False),
            param(nan, False),
        ],
    )
    def test_is_finite_and_non_negative(
        self, *, x: float, expected: bool
    ) -> None:
        assert is_finite_and_non_negative(x, abs_tol=1e-8) is expected

    def test_is_finite_and_non_negative_or_nan(self) -> None:
        assert is_finite_and_non_negative_or_nan(nan)

    @mark.parametrize(
        ("x", "expected"),
        [
            param(-inf, False),
            param(-1.0, True),
            param(-1e-6, True),
            param(-1e-7, True),
            param(-1e-8, True),
            param(0.0, True),
            param(1e-8, True),
            param(1e-7, False),
            param(1e-6, False),
            param(1.0, False),
            param(inf, False),
            param(nan, False),
        ],
    )
    def test_is_finite_and_non_positive(
        self, *, x: float, expected: bool
    ) -> None:
        assert is_finite_and_non_positive(x, abs_tol=1e-8) is expected

    def test_is_finite_and_non_positive_or_nan(self) -> None:
        assert is_finite_and_non_positive_or_nan(nan)

    @mark.parametrize(
        ("x", "expected"),
        [
            param(-inf, False),
            param(-1.0, True),
            param(-1e-6, True),
            param(-1e-7, True),
            param(-1e-8, False),
            param(0.0, False),
            param(1e-8, False),
            param(1e-7, True),
            param(1e-6, True),
            param(1.0, True),
            param(inf, False),
            param(nan, False),
        ],
    )
    def test_is_finite_and_non_zero(self, *, x: float, expected: bool) -> None:
        assert is_finite_and_non_zero(x, abs_tol=1e-8) is expected

    def test_is_finite_and_non_zero_or_nan(self) -> None:
        assert is_finite_and_non_zero_or_nan(nan)

    @mark.parametrize(
        ("x", "expected"),
        [
            param(-inf, False),
            param(-1.0, False),
            param(-1e-6, False),
            param(-1e-7, False),
            param(-1e-8, False),
            param(0.0, False),
            param(1e-8, False),
            param(1e-7, True),
            param(1e-6, True),
            param(1.0, True),
            param(inf, False),
            param(nan, False),
        ],
    )
    def test_is_finite_and_positive(self, *, x: float, expected: bool) -> None:
        assert is_finite_and_positive(x, abs_tol=1e-8) is expected

    def test_is_finite_and_positive_or_nan(self) -> None:
        assert is_finite_and_positive_or_nan(nan)

    @mark.parametrize(
        ("x", "expected"),
        [
            param(-inf, False),
            param(-1.0, True),
            param(0.0, True),
            param(1.0, True),
            param(inf, False),
            param(nan, True),
        ],
    )
    def test_is_finite_or_nan(self, *, x: float, expected: bool) -> None:
        assert is_finite_or_nan(x) is expected

    @mark.parametrize(
        ("x", "y", "expected"),
        [
            param(0.0, -inf, True),
            param(0.0, -1.0, True),
            param(0.0, -1e-6, True),
            param(0.0, -1e-7, True),
            param(0.0, -1e-8, False),
            param(0.0, 0.0, False),
            param(0.0, 1e-8, False),
            param(0.0, 1e-7, False),
            param(0.0, 1e-6, False),
            param(0.0, 1.0, False),
            param(0.0, inf, False),
            param(0.0, nan, False),
        ],
    )
    def test_is_greater_than(
        self, *, x: float, y: float, expected: bool
    ) -> None:
        assert is_greater_than(x, y, abs_tol=1e-8) is expected

    @mark.parametrize(
        "y",
        [
            param(-inf),
            param(-1.0),
            param(0.0),
            param(1.0),
            param(inf),
            param(nan),
        ],
    )
    def test_is_greater_than_or_nan(self, *, y: float) -> None:
        assert is_greater_than_or_nan(nan, y)

    @mark.parametrize(
        ("x", "expected"),
        [
            param(-inf, True),
            param(-2.0, True),
            param(-1.5, False),
            param(-1.0, True),
            param(-0.5, False),
            param(-1e-6, False),
            param(-1e-7, False),
            param(-1e-8, True),
            param(0.0, True),
            param(1e-8, True),
            param(1e-7, False),
            param(1e-6, False),
            param(0.5, False),
            param(1.0, True),
            param(1.5, False),
            param(2.0, True),
            param(inf, True),
            param(nan, False),
        ],
    )
    def test_is_integral(self, *, x: float, expected: bool) -> None:
        assert is_integral(x, abs_tol=1e-8) is expected

    def test_is_integral_or_nan(self) -> None:
        assert is_integral_or_nan(nan)

    @mark.parametrize(
        ("x", "y", "expected"),
        [
            param(0.0, -inf, False),
            param(0.0, -1.0, False),
            param(0.0, -1e-6, False),
            param(0.0, -1e-7, False),
            param(0.0, -1e-8, False),
            param(0.0, 0.0, False),
            param(0.0, 1e-8, False),
            param(0.0, 1e-7, True),
            param(0.0, 1e-6, True),
            param(0.0, 1.0, True),
            param(0.0, inf, True),
            param(0.0, nan, False),
        ],
    )
    def test_is_less_than(self, *, x: float, y: float, expected: bool) -> None:
        assert is_less_than(x, y, abs_tol=1e-8) is expected

    @mark.parametrize(
        "y",
        [
            param(-inf),
            param(-1.0),
            param(0.0),
            param(1.0),
            param(inf),
            param(nan),
        ],
    )
    def test_is_less_than_or_nan(self, *, y: float) -> None:
        assert is_less_than_or_nan(nan, y)

    @mark.parametrize(
        ("x", "expected"),
        [
            param(-inf, True),
            param(-1.0, True),
            param(-1e-6, True),
            param(-1e-7, True),
            param(-1e-8, False),
            param(0.0, False),
            param(1e-8, False),
            param(1e-7, False),
            param(1e-6, False),
            param(1.0, False),
            param(inf, False),
            param(nan, False),
        ],
    )
    def test_is_negative(self, *, x: float, expected: bool) -> None:
        assert is_negative(x, abs_tol=1e-8) is expected

    def test_is_negative_or_nan(self) -> None:
        assert is_negative_or_nan(nan)

    @mark.parametrize(
        ("x", "expected"),
        [
            param(-inf, False),
            param(-1.0, False),
            param(-1e-6, False),
            param(-1e-7, False),
            param(-1e-8, True),
            param(0.0, True),
            param(1e-8, True),
            param(1e-7, True),
            param(1e-6, True),
            param(1.0, True),
            param(inf, True),
            param(nan, False),
        ],
    )
    def test_is_non_negative(self, *, x: float, expected: bool) -> None:
        assert is_non_negative(x, abs_tol=1e-8) is expected

    def test_is_non_negative_or_nan(self) -> None:
        assert is_non_negative_or_nan(nan)

    @mark.parametrize(
        ("x", "expected"),
        [
            param(-inf, True),
            param(-1.0, True),
            param(-1e-6, True),
            param(-1e-7, True),
            param(-1e-8, True),
            param(0.0, True),
            param(1e-8, True),
            param(1e-7, False),
            param(1e-6, False),
            param(1.0, False),
            param(inf, False),
            param(nan, False),
        ],
    )
    def test_is_non_positive(self, *, x: float, expected: bool) -> None:
        assert is_non_positive(x, abs_tol=1e-8) is expected

    def test_is_non_positive_or_nan(self) -> None:
        assert is_non_positive_or_nan(nan)

    @mark.parametrize(
        ("x", "expected"),
        [
            param(-inf, True),
            param(-1.0, True),
            param(-1e-6, True),
            param(-1e-7, True),
            param(-1e-8, False),
            param(0.0, False),
            param(1e-8, False),
            param(1e-7, True),
            param(1e-6, True),
            param(1.0, True),
            param(inf, True),
            param(nan, True),
        ],
    )
    def test_is_non_zero(self, *, x: float, expected: bool) -> None:
        assert is_non_zero(x, abs_tol=1e-8) is expected

    def test_is_non_zero_or_nan(self) -> None:
        assert is_non_zero_or_nan(nan)

    @mark.parametrize(
        ("x", "expected"),
        [
            param(-inf, False),
            param(-1.0, False),
            param(-1e-6, False),
            param(-1e-7, False),
            param(-1e-8, False),
            param(0.0, False),
            param(1e-8, False),
            param(1e-7, True),
            param(1e-6, True),
            param(1.0, True),
            param(inf, True),
            param(nan, False),
        ],
    )
    def test_is_positive(self, *, x: float, expected: bool) -> None:
        assert is_positive(x, abs_tol=1e-8) is expected

    def test_is_positive_or_nan(self) -> None:
        assert is_positive_or_nan(nan)

    @mark.parametrize(
        ("x", "expected"),
        [
            param(-inf, False),
            param(-1.0, False),
            param(-1e-6, False),
            param(-1e-7, False),
            param(-1e-8, True),
            param(0.0, True),
            param(1e-8, True),
            param(1e-7, False),
            param(1e-6, False),
            param(1.0, False),
            param(inf, False),
            param(nan, False),
        ],
    )
    def test_is_zero(self, *, x: float, expected: bool) -> None:
        assert is_zero(x, abs_tol=1e-8) is expected

    @mark.parametrize(
        ("x", "expected"),
        [
            param(-inf, False),
            param(-1.0, True),
            param(-1e-6, True),
            param(-1e-7, True),
            param(-1e-8, False),
            param(0.0, True),
            param(1e-8, False),
            param(1e-7, True),
            param(1e-6, True),
            param(1.0, True),
            param(inf, False),
            param(nan, False),
        ],
    )
    def test_is_zero_or_finite_and_non_micro(
        self, *, x: float, expected: bool
    ) -> None:
        assert is_zero_or_finite_and_non_micro(x, abs_tol=1e-8) is expected

    def test_is_zero_or_finite_and_non_micro_or_nan(self) -> None:
        assert is_zero_or_finite_and_non_micro_or_nan(nan)

    def test_is_zero_or_nan(self) -> None:
        assert is_zero_or_nan(nan)

    @mark.parametrize(
        ("x", "expected"),
        [
            param(-inf, True),
            param(-1.0, True),
            param(-1e-6, True),
            param(-1e-7, True),
            param(-1e-8, False),
            param(0.0, True),
            param(1e-8, False),
            param(1e-7, True),
            param(1e-6, True),
            param(1.0, True),
            param(inf, True),
            param(nan, True),
        ],
    )
    def test_is_zero_or_non_micro(self, *, x: float, expected: bool) -> None:
        assert is_zero_or_non_micro(x, abs_tol=1e-8) is expected

    def test_is_zero_or_non_micro_or_nan(self) -> None:
        assert is_zero_or_non_micro_or_nan(nan)
