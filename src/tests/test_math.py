from __future__ import annotations

from math import inf, nan
from re import escape
from typing import Any, ClassVar

from hypothesis import given
from hypothesis.strategies import integers, sampled_from
from numpy import iinfo, int8, int16, int32, int64, uint8, uint16, uint32, uint64
from pytest import approx, mark, param, raises

from utilities.hypothesis import pairs
from utilities.math import (
    MAX_INT8,
    MAX_INT16,
    MAX_INT32,
    MAX_INT64,
    MAX_UINT8,
    MAX_UINT16,
    MAX_UINT32,
    MAX_UINT64,
    MIN_INT8,
    MIN_INT16,
    MIN_INT32,
    MIN_INT64,
    MIN_UINT8,
    MIN_UINT16,
    MIN_UINT32,
    MIN_UINT64,
    CheckIntegerError,
    NumberOfDecimalsError,
    SafeRoundError,
    _EWMParameters,
    _EWMParametersAlphaError,
    _EWMParametersArgumentsError,
    _EWMParametersCOMError,
    _EWMParametersHalfLifeError,
    _EWMParametersSpanError,
    _RoundMode,
    check_integer,
    ewm_parameters,
    is_at_least,
    is_at_least_or_nan,
    is_at_most,
    is_at_most_or_nan,
    is_between,
    is_between_or_nan,
    is_equal,
    is_equal_or_approx,
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
    number_of_decimals,
    order_of_magnitude,
    round_,
    round_to_float,
    safe_round,
    sign,
)


class TestCheckInteger:
    def test_equal_pass(self) -> None:
        check_integer(0, equal=0)

    def test_equal_fail(self) -> None:
        with raises(CheckIntegerError, match="Integer must be equal to .*; got .*"):
            check_integer(0, equal=1)

    @given(equal_or_approx=sampled_from([10, (11, 0.1)]))
    def test_equal_or_approx_pass(
        self, *, equal_or_approx: int | tuple[int, float]
    ) -> None:
        check_integer(10, equal_or_approx=equal_or_approx)

    @given(
        case=sampled_from([
            (10, "Integer must be equal to .*; got .*"),
            (
                (11, 0.1),
                r"Integer must be approximately equal to .* \(error .*\); got .*",
            ),
        ])
    )
    def test_equal_or_approx_fail(
        self, *, case: tuple[int | tuple[int, float], str]
    ) -> None:
        equal_or_approx, match = case
        with raises(CheckIntegerError, match=match):
            check_integer(0, equal_or_approx=equal_or_approx)

    def test_min_pass(self) -> None:
        check_integer(0, min=0)

    def test_min_error(self) -> None:
        with raises(CheckIntegerError, match="Integer must be at least .*; got .*"):
            check_integer(0, min=1)

    def test_max_pass(self) -> None:
        check_integer(0, max=1)

    def test_max_error(self) -> None:
        with raises(CheckIntegerError, match="Integer must be at most .*; got .*"):
            check_integer(1, max=0)


class TestEWMParameters:
    expected: ClassVar[_EWMParameters] = _EWMParameters(
        com=1.0, span=3.0, half_life=1.0, alpha=0.5
    )

    def test_com(self) -> None:
        result = ewm_parameters(com=1.0)
        assert result == self.expected

    def test_span(self) -> None:
        result = ewm_parameters(span=3.0)
        assert result == self.expected

    def test_half_life(self) -> None:
        result = ewm_parameters(half_life=1.0)
        assert result == self.expected

    def test_alpha(self) -> None:
        result = ewm_parameters(alpha=0.5)
        assert result == self.expected

    def test_error_com(self) -> None:
        with raises(
            _EWMParametersCOMError,
            match=escape(r"Center of mass (γ) must be positive; got 0.0"),  # noqa: RUF001
        ):
            _ = ewm_parameters(com=0.0)

    def test_error_span(self) -> None:
        with raises(
            _EWMParametersSpanError,
            match=escape("Span (θ) must be greater than 1; got 1.0"),
        ):
            _ = ewm_parameters(span=1.0)

    def test_error_half_life(self) -> None:
        with raises(
            _EWMParametersHalfLifeError,
            match=escape("Half-life (λ) must be positive; got 0.0"),
        ):
            _ = ewm_parameters(half_life=0.0)

    @given(alpha=sampled_from([0.0, 1.0]))
    def test_error_alpha(self, *, alpha: float) -> None:
        with raises(
            _EWMParametersAlphaError,
            match=r"Smoothing factor \(α\) must be between 0 and 1 \(exclusive\); got \d\.0",  # noqa: RUF001
        ):
            _ = ewm_parameters(alpha=alpha)

    def test_error_arguments(self) -> None:
        with raises(
            _EWMParametersArgumentsError,
            match=escape(
                r"Exactly one of center of mass (γ), span (θ), half-life (λ) or smoothing factor (α) must be given; got γ=None, θ=None, λ=None and α=None"  # noqa: RUF001
            ),
        ):
            _ = ewm_parameters()


class TestIsAtLeast:
    @given(
        case=sampled_from([
            (0.0, -inf, True),
            (0.0, -1.0, True),
            (0.0, -1e-6, True),
            (0.0, -1e-7, True),
            (0.0, -1e-8, True),
            (0.0, 0.0, True),
            (0.0, 1e-8, True),
            (0.0, 1e-7, False),
            (0.0, 1e-6, False),
            (0.0, 1.0, False),
            (0.0, inf, False),
            (0.0, nan, False),
        ])
    )
    def test_main(self, *, case: tuple[float, float, bool]) -> None:
        x, y, expected = case
        assert is_at_least(x, y, abs_tol=1e-8) is expected

    @given(y=sampled_from([-inf, -1.0, 0.0, 1.0, inf, nan]))
    def test_nan(self, *, y: float) -> None:
        assert is_at_least_or_nan(nan, y)


class TestIsAtMost:
    @given(
        case=sampled_from([
            (0.0, -inf, False),
            (0.0, -1.0, False),
            (0.0, -1e-6, False),
            (0.0, -1e-7, False),
            (0.0, -1e-8, True),
            (0.0, 0.0, True),
            (0.0, 1e-8, True),
            (0.0, 1e-7, True),
            (0.0, 1e-6, True),
            (0.0, 1.0, True),
            (0.0, inf, True),
            (0.0, nan, False),
        ])
    )
    def test_main(self, *, case: tuple[float, float, bool]) -> None:
        x, y, expected = case
        assert is_at_most(x, y, abs_tol=1e-8) is expected

    @given(y=sampled_from([-inf, -1.0, 0.0, 1.0, inf, nan]))
    def test_nan(self, *, y: float) -> None:
        assert is_at_most_or_nan(nan, y)


class TestIsBetween:
    @given(
        case=sampled_from([
            (0.0, -1.0, -1.0, False),
            (0.0, -1.0, 0.0, True),
            (0.0, -1.0, 1.0, True),
            (0.0, 0.0, -1.0, False),
            (0.0, 0.0, 0.0, True),
            (0.0, 0.0, 1.0, True),
            (0.0, 1.0, -1.0, False),
            (0.0, 1.0, 0.0, False),
            (0.0, 1.0, 1.0, False),
            (nan, -1.0, 1.0, False),
        ])
    )
    def test_main(self, *, case: tuple[float, float, float, bool]) -> None:
        x, low, high, expected = case
        assert is_between(x, low, high, abs_tol=1e-8) is expected

    @given(bounds=pairs(sampled_from([-inf, -1.0, 0.0, 1.0, inf, nan])))
    def test_nan(self, *, bounds: tuple[float, float]) -> None:
        low, high = bounds
        assert is_between_or_nan(nan, low, high)


class TestIsEqual:
    @given(case=sampled_from([(-1, False), (0, True), (1, False)]))
    def test_two_ints(self, *, case: tuple[float, bool]) -> None:
        x, expected = case
        assert is_equal(x, 0) is expected
        assert is_equal(0, x) is expected

    @given(
        case=sampled_from([
            (-inf, False),
            (-1.0, False),
            (-1e-6, False),
            (-1e-7, False),
            (-1e-8, False),
            (0.0, True),
            (1e-8, False),
            (1e-7, False),
            (1e-6, False),
            (1.0, False),
            (inf, False),
            (nan, False),
        ])
    )
    def test_one_float(self, *, case: tuple[float, bool]) -> None:
        x, expected = case
        assert is_equal(x, 0.0) is expected
        assert is_equal(0.0, x) is expected

    @given(
        case=sampled_from([
            (nan, nan, True),
            (nan, inf, False),
            (nan, -inf, False),
            (inf, inf, True),
            (inf, -inf, False),
            (-inf, -inf, True),
        ])
    )
    def test_special(self, *, case: tuple[float, float, bool]) -> None:
        x, y, expected = case
        assert is_equal(x, y) is expected
        assert is_equal(y, x) is expected


class TestIsEqualOrApprox:
    @given(
        case=sampled_from([
            (0, 0, True),
            (0, 1, False),
            (1, 0, False),
            (10, (8, 0.1), False),
            (10, (9, 0.1), True),
            (10, (10, 0.1), True),
            (10, (11, 0.1), True),
            (10, (12, 0.1), False),
            ((10, 0.1), (8, 0.1), False),
            ((10, 0.1), (9, 0.1), True),
            ((10, 0.1), (10, 0.1), True),
            ((10, 0.1), (11, 0.1), True),
            ((10, 0.1), (12, 0.1), False),
        ])
    )
    def test_main(
        self, *, case: tuple[int | tuple[int, float], int | tuple[int, float], bool]
    ) -> None:
        x, y, expected = case
        assert is_equal_or_approx(x, y) is expected
        assert is_equal_or_approx(y, x) is expected


class TestIsFinite:
    @given(
        case=sampled_from([
            (-inf, False, False),
            (-1.0, True, True),
            (0.0, True, True),
            (1.0, True, True),
            (inf, False, False),
            (nan, False, True),
        ])
    )
    def test_main(self, *, case: tuple[float, bool, bool]) -> None:
        x, expected, expected_nan = case
        assert is_finite(x) is expected
        assert is_finite_or_nan(x) is expected_nan


class TestIsFiniteAndIntegral:
    @given(
        case=sampled_from([
            (-inf, False, False),
            (-2.0, True, True),
            (-1.5, False, False),
            (-1.0, True, True),
            (-0.5, False, False),
            (-1e-6, False, False),
            (-1e-7, False, False),
            (-1e-8, True, True),
            (0.0, True, True),
            (1e-8, True, True),
            (1e-7, False, False),
            (1e-6, False, False),
            (0.5, False, False),
            (1.0, True, True),
            (1.5, False, False),
            (2.0, True, True),
            (inf, False, False),
            (nan, False, True),
        ])
    )
    def test_main(self, *, case: tuple[float, bool, bool]) -> None:
        x, expected, expected_nan = case
        assert is_finite_and_integral(x, abs_tol=1e-8) is expected
        assert is_finite_and_integral_or_nan(x, abs_tol=1e-8) is expected_nan


class TestIsFiniteAndNegative:
    @given(
        case=sampled_from([
            (-inf, False, False),
            (-1.0, True, True),
            (-1e-6, True, True),
            (-1e-7, True, True),
            (-1e-8, False, False),
            (0.0, False, False),
            (1e-8, False, False),
            (1e-7, False, False),
            (1e-6, False, False),
            (1.0, False, False),
            (inf, False, False),
            (nan, False, True),
        ])
    )
    def test_main(self, *, case: tuple[float, bool, bool]) -> None:
        x, expected, expected_nan = case
        assert is_finite_and_negative(x, abs_tol=1e-8) is expected
        assert is_finite_and_negative_or_nan(x, abs_tol=1e-8) is expected_nan


class TestIsFiniteAndNonNegative:
    @given(
        case=sampled_from([
            (-inf, False, False),
            (-1.0, False, False),
            (-1e-6, False, False),
            (-1e-7, False, False),
            (-1e-8, True, True),
            (0.0, True, True),
            (1e-8, True, True),
            (1e-7, True, True),
            (1e-6, True, True),
            (1.0, True, True),
            (inf, False, False),
            (nan, False, True),
        ])
    )
    def test_main(self, *, case: tuple[float, bool, bool]) -> None:
        x, expected, expected_nan = case
        assert is_finite_and_non_negative(x, abs_tol=1e-8) is expected
        assert is_finite_and_non_negative_or_nan(x, abs_tol=1e-8) is expected_nan


class TestIsFiniteAndNonPositive:
    @given(
        case=sampled_from([
            (-inf, False, False),
            (-1.0, True, True),
            (-1e-6, True, True),
            (-1e-7, True, True),
            (-1e-8, True, True),
            (0.0, True, True),
            (1e-8, True, True),
            (1e-7, False, False),
            (1e-6, False, False),
            (1.0, False, False),
            (inf, False, False),
            (nan, False, True),
        ])
    )
    def test_main(self, *, case: tuple[float, bool, bool]) -> None:
        x, expected, expected_nan = case
        assert is_finite_and_non_positive(x, abs_tol=1e-8) is expected
        assert is_finite_and_non_positive_or_nan(x, abs_tol=1e-8) is expected_nan


class TestIsFiniteAndNonZero:
    @given(
        case=sampled_from([
            (-inf, False, False),
            (-1.0, True, True),
            (-1e-6, True, True),
            (-1e-7, True, True),
            (-1e-8, False, False),
            (0.0, False, False),
            (1e-8, False, False),
            (1e-7, True, True),
            (1e-6, True, True),
            (1.0, True, True),
            (inf, False, False),
            (nan, False, True),
        ])
    )
    def test_main(self, *, x: float, expected: bool, expected_nan: bool) -> None:
        assert is_finite_and_non_zero(x, abs_tol=1e-8) is expected
        assert is_finite_and_non_zero_or_nan(x, abs_tol=1e-8) is expected_nan


class TestIsFiniteAndPositive:
    @given(
        case=sampled_from([
            (-inf, False, False),
            (-1.0, False, False),
            (-1e-6, False, False),
            (-1e-7, False, False),
            (-1e-8, False, False),
            (0.0, False, False),
            (1e-8, False, False),
            (1e-7, True, True),
            (1e-6, True, True),
            (1.0, True, True),
            (inf, False, False),
            (nan, False, True),
        ])
    )
    def test_main(self, *, x: float, expected: bool, expected_nan: bool) -> None:
        assert is_finite_and_positive(x, abs_tol=1e-8) is expected
        assert is_finite_and_positive_or_nan(x, abs_tol=1e-8) is expected_nan


class TestIsGreaterThan:
    @given(
        case=sampled_from([
            (0.0, -inf, True),
            (0.0, -1.0, True),
            (0.0, -1e-6, True),
            (0.0, -1e-7, True),
            (0.0, -1e-8, False),
            (0.0, 0.0, False),
            (0.0, 1e-8, False),
            (0.0, 1e-7, False),
            (0.0, 1e-6, False),
            (0.0, 1.0, False),
            (0.0, inf, False),
            (0.0, nan, False),
        ])
    )
    def test_main(self, *, x: float, y: float, expected: bool) -> None:
        assert is_greater_than(x, y, abs_tol=1e-8) is expected

    @given(y=sampled_from([-inf, -1.0, 0.0, 1.0, inf, nan]))
    def test_nan(self, *, y: float) -> None:
        assert is_greater_than_or_nan(nan, y)


class TestIsIntegral:
    @given(
        case=sampled_from([
            (-inf, True, True),
            (-2.0, True, True),
            (-1.5, False, False),
            (-1.0, True, True),
            (-0.5, False, False),
            (-1e-6, False, False),
            (-1e-7, False, False),
            (-1e-8, True, True),
            (0.0, True, True),
            (1e-8, True, True),
            (1e-7, False, False),
            (1e-6, False, False),
            (0.5, False, False),
            (1.0, True, True),
            (1.5, False, False),
            (2.0, True, True),
            (inf, True, True),
            (nan, False, True),
        ])
    )
    def test_is_integral(self, *, x: float, expected: bool, expected_nan: bool) -> None:
        assert is_integral(x, abs_tol=1e-8) is expected
        assert is_integral_or_nan(x, abs_tol=1e-8) is expected_nan


class TestIsLessThan:
    @given(
        case=sampled_from([
            (0.0, -inf, False),
            (0.0, -1.0, False),
            (0.0, -1e-6, False),
            (0.0, -1e-7, False),
            (0.0, -1e-8, False),
            (0.0, 0.0, False),
            (0.0, 1e-8, False),
            (0.0, 1e-7, True),
            (0.0, 1e-6, True),
            (0.0, 1.0, True),
            (0.0, inf, True),
            (0.0, nan, False),
        ])
    )
    def test_main(self, *, x: float, y: float, expected: bool) -> None:
        assert is_less_than(x, y, abs_tol=1e-8) is expected

    @given(y=sampled_from([-inf, -1.0, 0.0, 1.0, inf, nan]))
    def test_nan(self, *, y: float) -> None:
        assert is_less_than_or_nan(nan, y)


class TestIsNegative:
    @given(
        case=sampled_from([
            (-inf, True, True),
            (-1.0, True, True),
            (-1e-6, True, True),
            (-1e-7, True, True),
            (-1e-8, False, False),
            (0.0, False, False),
            (1e-8, False, False),
            (1e-7, False, False),
            (1e-6, False, False),
            (1.0, False, False),
            (inf, False, False),
            (nan, False, True),
        ])
    )
    def test_main(self, *, x: float, expected: bool, expected_nan: bool) -> None:
        assert is_negative(x, abs_tol=1e-8) is expected
        assert is_negative_or_nan(x, abs_tol=1e-8) is expected_nan


class TestIsNonNegative:
    @given(
        case=sampled_from([
            (-inf, False, False),
            (-1.0, False, False),
            (-1e-6, False, False),
            (-1e-7, False, False),
            (-1e-8, True, True),
            (0.0, True, True),
            (1e-8, True, True),
            (1e-7, True, True),
            (1e-6, True, True),
            (1.0, True, True),
            (inf, True, True),
            (nan, False, True),
        ])
    )
    def test_main(self, *, x: float, expected: bool, expected_nan: bool) -> None:
        assert is_non_negative(x, abs_tol=1e-8) is expected
        assert is_non_negative_or_nan(x, abs_tol=1e-8) is expected_nan


class TestIsNonPositive:
    @given(
        case=sampled_from([
            (-inf, True, True),
            (-1.0, True, True),
            (-1e-6, True, True),
            (-1e-7, True, True),
            (-1e-8, True, True),
            (0.0, True, True),
            (1e-8, True, True),
            (1e-7, False, False),
            (1e-6, False, False),
            (1.0, False, False),
            (inf, False, False),
            (nan, False, True),
        ])
    )
    def test_main(self, *, x: float, expected: bool, expected_nan: bool) -> None:
        assert is_non_positive(x, abs_tol=1e-8) is expected
        assert is_non_positive_or_nan(x, abs_tol=1e-8) is expected_nan


class TestIsNonZero:
    @given(
        case=sampled_from([
            (-inf, True),
            (-1.0, True),
            (-1e-6, True),
            (-1e-7, True),
            (-1e-8, False),
            (0.0, False),
            (1e-8, False),
            (1e-7, True),
            (1e-6, True),
            (1.0, True),
            (inf, True),
            (nan, True),
        ])
    )
    def test_main(self, *, x: float, expected: bool) -> None:
        assert is_non_zero(x, abs_tol=1e-8) is expected
        assert is_non_zero_or_nan(x, abs_tol=1e-8) is expected


class TestIsPositive:
    @given(
        case=sampled_from([
            (-inf, False, False),
            (-1.0, False, False),
            (-1e-6, False, False),
            (-1e-7, False, False),
            (-1e-8, False, False),
            (0.0, False, False),
            (1e-8, False, False),
            (1e-7, True, True),
            (1e-6, True, True),
            (1.0, True, True),
            (inf, True, True),
            (nan, False, True),
        ])
    )
    def test_main(self, *, x: float, expected: bool, expected_nan: bool) -> None:
        assert is_positive(x, abs_tol=1e-8) is expected
        assert is_positive_or_nan(x, abs_tol=1e-8) is expected_nan


class TestIsZero:
    @given(
        case=sampled_from([
            (-inf, False, False),
            (-1.0, False, False),
            (-1e-6, False, False),
            (-1e-7, False, False),
            (-1e-8, True, True),
            (0.0, True, True),
            (1e-8, True, True),
            (1e-7, False, False),
            (1e-6, False, False),
            (1.0, False, False),
            (inf, False, False),
            (nan, False, True),
        ])
    )
    def test_main(self, *, x: float, expected: bool, expected_nan: bool) -> None:
        assert is_zero(x, abs_tol=1e-8) is expected
        assert is_zero_or_nan(x, abs_tol=1e-8) is expected_nan


class TestIsZeroOrFiniteAndNonMicro:
    @given(
        case=sampled_from([
            (-inf, False, False),
            (-1.0, True, True),
            (-1e-6, True, True),
            (-1e-7, True, True),
            (-1e-8, False, False),
            (0.0, True, True),
            (1e-8, False, False),
            (1e-7, True, True),
            (1e-6, True, True),
            (1.0, True, True),
            (inf, False, False),
            (nan, False, True),
        ])
    )
    def test_main(self, *, x: float, expected: bool, expected_nan: bool) -> None:
        assert is_zero_or_finite_and_non_micro(x, abs_tol=1e-8) is expected
        assert is_zero_or_finite_and_non_micro_or_nan(x, abs_tol=1e-8) is expected_nan


class TestIsZeroOrNonMicro:
    @given(
        case=sampled_from([
            (-inf, True),
            (-1.0, True),
            (-1e-6, True),
            (-1e-7, True),
            (-1e-8, False),
            (0.0, True),
            (1e-8, False),
            (1e-7, True),
            (1e-6, True),
            (1.0, True),
            (inf, True),
            (nan, True),
        ])
    )
    def test_main(self, *, x: float, expected: bool) -> None:
        assert is_zero_or_non_micro(x, abs_tol=1e-8) is expected
        assert is_zero_or_non_micro_or_nan(x, abs_tol=1e-8) is expected


class TestMaxLongAndDouble:
    @given(
        case=sampled_from([
            (MIN_INT8, MAX_INT8, int8),
            (MIN_INT16, MAX_INT16, int16),
            (MIN_INT32, MAX_INT32, int32),
            (MIN_INT64, MAX_INT64, int64),
            (MIN_UINT8, MAX_UINT8, uint8),
            (MIN_UINT16, MAX_UINT16, uint16),
            (MIN_UINT32, MAX_UINT32, uint32),
            (MIN_UINT64, MAX_UINT64, uint64),
        ])
    )
    def test_main(self, *, min_value: int, max_value: int, dtype: Any) -> None:
        info = iinfo(dtype)
        assert info.min == min_value
        assert info.max == max_value


class TestNumberOfDecimals:
    max_int: ClassVar[int] = int(1e6)

    @given(
        integer=integers(-max_int, max_int),
        case=sampled_from([
            (0.0, 0),
            (0.1, 1),
            (0.12, 2),
            (0.123, 3),
            (0.1234, 4),
            (0.12345, 5),
            (0.123456, 6),
            (0.1234567, 7),
            (0.12345678, 8),
            (0.123456789, 9),
        ]),
    )
    def test_main(self, *, integer: int, frac: float, expected: int) -> None:
        x = integer + frac
        result = number_of_decimals(x)
        assert result == expected

    def test_equal_fail(self) -> None:
        x = 1.401298464324817e-45
        with raises(
            NumberOfDecimalsError,
            match=escape(
                "Could not determine number of decimals of 1.401298464324817e-45 (up to 20)"
            ),
        ):
            _ = number_of_decimals(x)


class TestOrderOfMagnitude:
    @given(
        sign=sampled_from([1, -1]),
        case=sampled_from([
            (0.25, -0.60206, -1),
            (0.5, -0.30103, 0),
            (0.75, -0.1249387, 0),
            (1.0, 0.0, 0),
            (5.0, 0.69897, 1),
            (10.0, 1.0, 1),
            (50.0, 1.69897, 2),
            (100.0, 2.0, 2),
        ]),
    )
    def test_main(self, *, sign: int, x: float, exp_float: float, exp_int: int) -> None:
        x_use = sign * x
        res_float = order_of_magnitude(x_use)
        assert res_float == approx(exp_float)
        res_int = order_of_magnitude(x_use, round_=True)
        assert res_int == exp_int


class TestRound:
    @given(
        case=sampled_from([
            ("standard", -2.0, -2),
            ("standard", -1.75, -2),
            ("standard", -1.5, -2),
            ("standard", -1.25, -1),
            ("standard", -1.0, -1),
            ("standard", -0.75, -1),
            ("standard", -0.5, 0),
            ("standard", -0.25, 0),
            ("standard", 0.0, 0),
            ("standard", 0.25, 0),
            ("standard", 0.5, 0),
            ("standard", 0.75, 1),
            ("standard", 1.0, 1),
            ("standard", 1.25, 1),
            ("standard", 1.5, 2),
            ("standard", 1.75, 2),
            ("standard", 2.0, 2),
            ("floor", -2.0, -2),
            ("floor", -1.75, -2),
            ("floor", -1.5, -2),
            ("floor", -1.25, -2),
            ("floor", -1.0, -1),
            ("floor", -0.75, -1),
            ("floor", -0.5, -1),
            ("floor", -0.25, -1),
            ("floor", 0.0, 0),
            ("floor", 0.25, 0),
            ("floor", 0.5, 0),
            ("floor", 0.75, 0),
            ("floor", 1.0, 1),
            ("floor", 1.25, 1),
            ("floor", 1.5, 1),
            ("floor", 1.75, 1),
            ("floor", 2.0, 2),
            ("ceil", -2.0, -2),
            ("ceil", -1.75, -1),
            ("ceil", -1.5, -1),
            ("ceil", -1.25, -1),
            ("ceil", -1.0, -1),
            ("ceil", -0.75, 0),
            ("ceil", -0.5, 0),
            ("ceil", -0.25, 0),
            ("ceil", 0.0, 0),
            ("ceil", 0.25, 1),
            ("ceil", 0.5, 1),
            ("ceil", 0.75, 1),
            ("ceil", 1.0, 1),
            ("ceil", 1.25, 2),
            ("ceil", 1.5, 2),
            ("ceil", 1.75, 2),
            ("ceil", 2.0, 2),
            ("toward-zero", -2.0, -2),
            ("toward-zero", -1.75, -1),
            ("toward-zero", -1.5, -1),
            ("toward-zero", -1.25, -1),
            ("toward-zero", -1.0, -1),
            ("toward-zero", -0.75, 0),
            ("toward-zero", -0.5, 0),
            ("toward-zero", -0.25, 0),
            ("toward-zero", 0.0, 0),
            ("toward-zero", 0.25, 0),
            ("toward-zero", 0.5, 0),
            ("toward-zero", 0.75, 0),
            ("toward-zero", 1.0, 1),
            ("toward-zero", 1.25, 1),
            ("toward-zero", 1.5, 1),
            ("toward-zero", 1.75, 1),
            ("toward-zero", 2.0, 2),
            ("away-zero", -2.0, -2),
            ("away-zero", -1.75, -2),
            ("away-zero", -1.5, -2),
            ("away-zero", -1.25, -2),
            ("away-zero", -1.0, -1),
            ("away-zero", -0.75, -1),
            ("away-zero", -0.5, -1),
            ("away-zero", -0.25, -1),
            ("away-zero", 0.0, 0),
            ("away-zero", 0.25, 1),
            ("away-zero", 0.5, 1),
            ("away-zero", 0.75, 1),
            ("away-zero", 1.0, 1),
            ("away-zero", 1.25, 2),
            ("away-zero", 1.5, 2),
            ("away-zero", 1.75, 2),
            ("away-zero", 2.0, 2),
            ("standard-tie-floor", -2.0, -2),
            ("standard-tie-floor", -1.75, -2),
            ("standard-tie-floor", -1.5, -2),
            ("standard-tie-floor", -1.25, -1),
            ("standard-tie-floor", -1.0, -1),
            ("standard-tie-floor", -0.75, -1),
            ("standard-tie-floor", -0.5, -1),
            ("standard-tie-floor", -0.25, 0),
            ("standard-tie-floor", 0.0, 0),
            ("standard-tie-floor", 0.25, 0),
            ("standard-tie-floor", 0.5, 0),
            ("standard-tie-floor", 0.75, 1),
            ("standard-tie-floor", 1.0, 1),
            ("standard-tie-floor", 1.25, 1),
            ("standard-tie-floor", 1.5, 1),
            ("standard-tie-floor", 1.75, 2),
            ("standard-tie-floor", 2.0, 2),
            ("standard-tie-ceil", -2.0, -2),
            ("standard-tie-ceil", -1.75, -2),
            ("standard-tie-ceil", -1.5, -1),
            ("standard-tie-ceil", -1.25, -1),
            ("standard-tie-ceil", -1.0, -1),
            ("standard-tie-ceil", -0.75, -1),
            ("standard-tie-ceil", -0.5, 0),
            ("standard-tie-ceil", -0.25, 0),
            ("standard-tie-ceil", 0.0, 0),
            ("standard-tie-ceil", 0.25, 0),
            ("standard-tie-ceil", 0.5, 1),
            ("standard-tie-ceil", 0.75, 1),
            ("standard-tie-ceil", 1.0, 1),
            ("standard-tie-ceil", 1.25, 1),
            ("standard-tie-ceil", 1.5, 2),
            ("standard-tie-ceil", 1.75, 2),
            ("standard-tie-ceil", 2.0, 2),
            ("standard-tie-toward-zero", -2.0, -2),
            ("standard-tie-toward-zero", -1.75, -2),
            ("standard-tie-toward-zero", -1.5, -1),
            ("standard-tie-toward-zero", -1.25, -1),
            ("standard-tie-toward-zero", -1.0, -1),
            ("standard-tie-toward-zero", -0.75, -1),
            ("standard-tie-toward-zero", -0.5, 0),
            ("standard-tie-toward-zero", -0.25, 0),
            ("standard-tie-toward-zero", 0.0, 0),
            ("standard-tie-toward-zero", 0.25, 0),
            ("standard-tie-toward-zero", 0.5, 0),
            ("standard-tie-toward-zero", 0.75, 1),
            ("standard-tie-toward-zero", 1.0, 1),
            ("standard-tie-toward-zero", 1.25, 1),
            ("standard-tie-toward-zero", 1.5, 1),
            ("standard-tie-toward-zero", 1.75, 2),
            ("standard-tie-toward-zero", 2.0, 2),
            ("standard-tie-away-zero", -2.0, -2),
            ("standard-tie-away-zero", -1.75, -2),
            ("standard-tie-away-zero", -1.5, -2),
            ("standard-tie-away-zero", -1.25, -1),
            ("standard-tie-away-zero", -1.0, -1),
            ("standard-tie-away-zero", -0.75, -1),
            ("standard-tie-away-zero", -0.5, -1),
            ("standard-tie-away-zero", -0.25, 0),
            ("standard-tie-away-zero", 0.0, 0),
            ("standard-tie-away-zero", 0.25, 0),
            ("standard-tie-away-zero", 0.5, 1),
            ("standard-tie-away-zero", 0.75, 1),
            ("standard-tie-away-zero", 1.0, 1),
            ("standard-tie-away-zero", 1.25, 1),
            ("standard-tie-away-zero", 1.5, 2),
            ("standard-tie-away-zero", 1.75, 2),
            ("standard-tie-away-zero", 2.0, 2),
        ])
    )
    def test_main(self, *, mode: _RoundMode, x: float, expected: int) -> None:
        result = round_(x, mode=mode)
        assert isinstance(result, int)
        assert result == expected


class TestRoundToFloat:
    @given(
        case=sampled_from([
            (0.0, 0.5, 0.0),
            (0.1, 0.5, 0.0),
            (0.2, 0.5, 0.0),
            (0.3, 0.5, 0.5),
            (0.4, 0.5, 0.5),
            (0.5, 0.5, 0.5),
            (0.6, 0.5, 0.5),
            (0.7, 0.5, 0.5),
            (0.8, 0.5, 1.0),
            (0.9, 0.5, 1.0),
            (1.0, 0.5, 1.0),
            (1.1, 0.5, 1.0),
            (1.2, 0.5, 1.0),
            (1.3, 0.5, 1.5),
            (1.4, 0.5, 1.5),
            (1.5, 0.5, 1.5),
            (1.6, 0.5, 1.5),
            (1.7, 0.5, 1.5),
            (1.8, 0.5, 2.0),
            (1.9, 0.5, 2.0),
            (2.0, 0.5, 2.0),
        ])
    )
    def test_main(self, *, x: float, y: float, expected: float) -> None:
        result = round_to_float(x, y)
        assert result == approx(expected)


class TestSafeRound:
    @given(case=sampled_from([(-2.0, -2), (-1.0, -1), (0.0, 0), (1.0, 1), (2.0, 2)]))
    def test_main(self, *, x: float, expected: int) -> None:
        result = safe_round(x)
        assert isinstance(result, int)
        assert result == expected

    @given(case=sampled_from([-inf, -1.5, -0.5, 0.5, 1.5, inf, nan]))
    def test_error(self, *, x: float) -> None:
        with raises(
            SafeRoundError,
            match=r"Unable to safely round .* \(rel_tol=.*, abs_tol=.*\)",
        ):
            _ = safe_round(x)


class TestSign:
    @mark.parametrize(
        ("x", "expected"),
        [
            param(-3, -1),
            param(-2, -1),
            param(-1, -1),
            param(0, 0),
            param(1, 1),
            param(2, 1),
            param(3, 1),
        ],
        ids=str,
    )
    def test_int(self, *, x: int, expected: float) -> None:
        result = sign(x)
        assert result == expected

    @mark.parametrize(
        ("x", "expected"),
        [
            param(-2.0, -1),
            param(-1.75, -1),
            param(-1.5, -1),
            param(-1.25, -1),
            param(-1.0, -1),
            param(-0.75, -1),
            param(-0.5, -1),
            param(-0.25, -1),
            param(0.0, 0),
            param(0.25, 1),
            param(0.5, 1),
            param(0.75, 1),
            param(1.0, 1),
            param(1.25, 1),
            param(1.5, 1),
            param(1.75, 1),
            param(2.0, 1),
        ],
        ids=str,
    )
    def test_float(self, *, x: float, expected: float) -> None:
        result = sign(x)
        assert result == expected
