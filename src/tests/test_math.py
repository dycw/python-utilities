from __future__ import annotations

from contextlib import suppress
from math import inf, nan
from typing import Any

from beartype.door import die_if_unbearable
from beartype.roar import BeartypeDoorHintViolation
from hypothesis import Phase, given, settings
from hypothesis.strategies import floats, integers
from pytest import mark, param

from utilities.math import (
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
    is_at_least,
    is_at_least_or_nan,
    is_at_most,
    is_at_most_or_nan,
    is_between,
    is_between_or_nan,
    is_finite,
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
    is_greater_than,
    is_greater_than_or_nan,
    is_integral,
    is_integral_or_nan,
    is_less_than,
    is_less_than_or_nan,
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
        "y", [param(-inf), param(-1.0), param(0.0), param(1.0), param(inf), param(nan)]
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
        "y", [param(-inf), param(-1.0), param(0.0), param(1.0), param(inf), param(nan)]
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
        [param(-inf), param(-1.0), param(0.0), param(1.0), param(inf), param(nan)],
    )
    @mark.parametrize(
        "high",
        [param(-inf), param(-1.0), param(0.0), param(1.0), param(inf), param(nan)],
    )
    def test_is_between_or_nan(self, *, low: float, high: float) -> None:
        assert is_between_or_nan(nan, low, high)

    @mark.parametrize(
        ("x", "expected"),
        [
            param(-inf, False),
            param(-1.0, True),
            param(0.0, True),
            param(1.0, True),
            param(inf, False),
            param(nan, False),
        ],
    )
    def test_is_finite(self, *, x: float, expected: bool) -> None:
        assert is_finite(x) is expected

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
    def test_is_finite_and_non_negative(self, *, x: float, expected: bool) -> None:
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
    def test_is_finite_and_non_positive(self, *, x: float, expected: bool) -> None:
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
    def test_is_greater_than(self, *, x: float, y: float, expected: bool) -> None:
        assert is_greater_than(x, y, abs_tol=1e-8) is expected

    @mark.parametrize(
        "y", [param(-inf), param(-1.0), param(0.0), param(1.0), param(inf), param(nan)]
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
        "y", [param(-inf), param(-1.0), param(0.0), param(1.0), param(inf), param(nan)]
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
    def test_is_zero_or_finite_and_non_micro(self, *, x: float, expected: bool) -> None:
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
