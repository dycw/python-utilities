from __future__ import annotations

from math import inf, nan

import pytest

from utilities.math import (
    CheckIntegerError,
    check_integer,
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
)


class TestCheckInteger:
    def test_equal_pass(self) -> None:
        check_integer(0, equal=0)

    def test_equal_fail(self) -> None:
        with pytest.raises(
            CheckIntegerError, match="Integer must be equal to .*; got .*"
        ):
            check_integer(0, equal=1)

    @pytest.mark.parametrize(
        "equal_or_approx", [pytest.param(10), pytest.param((11, 0.1))]
    )
    def test_equal_or_approx_pass(
        self, *, equal_or_approx: int | tuple[int, float]
    ) -> None:
        check_integer(10, equal_or_approx=equal_or_approx)

    @pytest.mark.parametrize(
        ("equal_or_approx", "match"),
        [
            pytest.param(10, "Integer must be equal to .*; got .*"),
            pytest.param(
                (11, 0.1),
                r"Integer must be approximately equal to .* \(error .*\); got .*",
            ),
        ],
    )
    def test_equal_or_approx_fail(
        self, *, equal_or_approx: int | tuple[int, float], match: str
    ) -> None:
        with pytest.raises(CheckIntegerError, match=match):
            check_integer(0, equal_or_approx=equal_or_approx)

    def test_min_pass(self) -> None:
        check_integer(0, min=0)

    def test_min_error(self) -> None:
        with pytest.raises(
            CheckIntegerError, match="Integer must be at least .*; got .*"
        ):
            check_integer(0, min=1)

    def test_max_pass(self) -> None:
        check_integer(0, max=1)

    def test_max_error(self) -> None:
        with pytest.raises(
            CheckIntegerError, match="Integer must be at most .*; got .*"
        ):
            check_integer(1, max=0)


class TestIsAtLeast:
    @pytest.mark.parametrize(
        ("x", "y", "expected"),
        [
            pytest.param(0.0, -inf, True),
            pytest.param(0.0, -1.0, True),
            pytest.param(0.0, -1e-6, True),
            pytest.param(0.0, -1e-7, True),
            pytest.param(0.0, -1e-8, True),
            pytest.param(0.0, 0.0, True),
            pytest.param(0.0, 1e-8, True),
            pytest.param(0.0, 1e-7, False),
            pytest.param(0.0, 1e-6, False),
            pytest.param(0.0, 1.0, False),
            pytest.param(0.0, inf, False),
            pytest.param(0.0, nan, False),
        ],
    )
    def test_main(self, *, x: float, y: float, expected: bool) -> None:
        assert is_at_least(x, y, abs_tol=1e-8) is expected

    @pytest.mark.parametrize(
        "y",
        [
            pytest.param(-inf),
            pytest.param(-1.0),
            pytest.param(0.0),
            pytest.param(1.0),
            pytest.param(inf),
            pytest.param(nan),
        ],
    )
    def test_nan(self, *, y: float) -> None:
        assert is_at_least_or_nan(nan, y)


class TestIsAtMost:
    @pytest.mark.parametrize(
        ("x", "y", "expected"),
        [
            pytest.param(0.0, -inf, False),
            pytest.param(0.0, -1.0, False),
            pytest.param(0.0, -1e-6, False),
            pytest.param(0.0, -1e-7, False),
            pytest.param(0.0, -1e-8, True),
            pytest.param(0.0, 0.0, True),
            pytest.param(0.0, 1e-8, True),
            pytest.param(0.0, 1e-7, True),
            pytest.param(0.0, 1e-6, True),
            pytest.param(0.0, 1.0, True),
            pytest.param(0.0, inf, True),
            pytest.param(0.0, nan, False),
        ],
    )
    def test_main(self, *, x: float, y: float, expected: bool) -> None:
        assert is_at_most(x, y, abs_tol=1e-8) is expected

    @pytest.mark.parametrize(
        "y",
        [
            pytest.param(-inf),
            pytest.param(-1.0),
            pytest.param(0.0),
            pytest.param(1.0),
            pytest.param(inf),
            pytest.param(nan),
        ],
    )
    def test_nan(self, *, y: float) -> None:
        assert is_at_most_or_nan(nan, y)


class TestIsBetween:
    @pytest.mark.parametrize(
        ("x", "low", "high", "expected"),
        [
            pytest.param(0.0, -1.0, -1.0, False),
            pytest.param(0.0, -1.0, 0.0, True),
            pytest.param(0.0, -1.0, 1.0, True),
            pytest.param(0.0, 0.0, -1.0, False),
            pytest.param(0.0, 0.0, 0.0, True),
            pytest.param(0.0, 0.0, 1.0, True),
            pytest.param(0.0, 1.0, -1.0, False),
            pytest.param(0.0, 1.0, 0.0, False),
            pytest.param(0.0, 1.0, 1.0, False),
            pytest.param(nan, -1.0, 1.0, False),
        ],
    )
    def test_main(self, *, x: float, low: float, high: float, expected: bool) -> None:
        assert is_between(x, low, high, abs_tol=1e-8) is expected

    @pytest.mark.parametrize(
        "low",
        [
            pytest.param(-inf),
            pytest.param(-1.0),
            pytest.param(0.0),
            pytest.param(1.0),
            pytest.param(inf),
            pytest.param(nan),
        ],
    )
    @pytest.mark.parametrize(
        "high",
        [
            pytest.param(-inf),
            pytest.param(-1.0),
            pytest.param(0.0),
            pytest.param(1.0),
            pytest.param(inf),
            pytest.param(nan),
        ],
    )
    def test_nan(self, *, low: float, high: float) -> None:
        assert is_between_or_nan(nan, low, high)


class TestIsEqual:
    @pytest.mark.parametrize(
        ("x", "y", "expected"),
        [
            pytest.param(0.0, -inf, False),
            pytest.param(0.0, -1.0, False),
            pytest.param(0.0, -1e-6, False),
            pytest.param(0.0, -1e-7, False),
            pytest.param(0.0, -1e-8, False),
            pytest.param(0.0, 0.0, True),
            pytest.param(0.0, 1e-8, False),
            pytest.param(0.0, 1e-7, False),
            pytest.param(0.0, 1e-6, False),
            pytest.param(0.0, 1.0, False),
            pytest.param(0.0, inf, False),
            pytest.param(0.0, nan, False),
        ],
    )
    def test_main(self, *, x: float, y: float, expected: bool) -> None:
        assert is_equal(x, y) is expected
        assert is_equal(y, x) is expected


class TestIsEqualOrApprox:
    @pytest.mark.parametrize(
        ("x", "y", "expected"),
        [
            pytest.param(0, 0, True),
            pytest.param(0, 1, False),
            pytest.param(1, 0, False),
            pytest.param(10, (8, 0.1), False),
            pytest.param(10, (9, 0.1), True),
            pytest.param(10, (10, 0.1), True),
            pytest.param(10, (11, 0.1), True),
            pytest.param(10, (12, 0.1), False),
            pytest.param((10, 0.1), (8, 0.1), False),
            pytest.param((10, 0.1), (9, 0.1), True),
            pytest.param((10, 0.1), (10, 0.1), True),
            pytest.param((10, 0.1), (11, 0.1), True),
            pytest.param((10, 0.1), (12, 0.1), False),
        ],
    )
    def test_main(
        self, *, x: int | tuple[int, float], y: int | tuple[int, float], expected: bool
    ) -> None:
        assert is_equal_or_approx(x, y) is expected
        assert is_equal_or_approx(y, x) is expected


class TestIsFinite:
    @pytest.mark.parametrize(
        ("x", "expected", "expected_nan"),
        [
            pytest.param(-inf, False, False),
            pytest.param(-1.0, True, True),
            pytest.param(0.0, True, True),
            pytest.param(1.0, True, True),
            pytest.param(inf, False, False),
            pytest.param(nan, False, True),
        ],
    )
    def test_main(self, *, x: float, expected: bool, expected_nan: bool) -> None:
        assert is_finite(x) is expected
        assert is_finite_or_nan(x) is expected_nan


class TestIsFiniteAndIntegral:
    @pytest.mark.parametrize(
        ("x", "expected", "expected_nan"),
        [
            pytest.param(-inf, False, False),
            pytest.param(-2.0, True, True),
            pytest.param(-1.5, False, False),
            pytest.param(-1.0, True, True),
            pytest.param(-0.5, False, False),
            pytest.param(-1e-6, False, False),
            pytest.param(-1e-7, False, False),
            pytest.param(-1e-8, True, True),
            pytest.param(0.0, True, True),
            pytest.param(1e-8, True, True),
            pytest.param(1e-7, False, False),
            pytest.param(1e-6, False, False),
            pytest.param(0.5, False, False),
            pytest.param(1.0, True, True),
            pytest.param(1.5, False, False),
            pytest.param(2.0, True, True),
            pytest.param(inf, False, False),
            pytest.param(nan, False, True),
        ],
    )
    def test_main(self, *, x: float, expected: bool, expected_nan: bool) -> None:
        assert is_finite_and_integral(x, abs_tol=1e-8) is expected
        assert is_finite_and_integral_or_nan(x, abs_tol=1e-8) is expected_nan


class TestIsFiniteAndNegative:
    @pytest.mark.parametrize(
        ("x", "expected", "expected_nan"),
        [
            pytest.param(-inf, False, False),
            pytest.param(-1.0, True, True),
            pytest.param(-1e-6, True, True),
            pytest.param(-1e-7, True, True),
            pytest.param(-1e-8, False, False),
            pytest.param(0.0, False, False),
            pytest.param(1e-8, False, False),
            pytest.param(1e-7, False, False),
            pytest.param(1e-6, False, False),
            pytest.param(1.0, False, False),
            pytest.param(inf, False, False),
            pytest.param(nan, False, True),
        ],
    )
    def test_main(self, *, x: float, expected: bool, expected_nan: bool) -> None:
        assert is_finite_and_negative(x, abs_tol=1e-8) is expected
        assert is_finite_and_negative_or_nan(x, abs_tol=1e-8) is expected_nan


class TestIsFiniteAndNonNegative:
    @pytest.mark.parametrize(
        ("x", "expected", "expected_nan"),
        [
            pytest.param(-inf, False, False),
            pytest.param(-1.0, False, False),
            pytest.param(-1e-6, False, False),
            pytest.param(-1e-7, False, False),
            pytest.param(-1e-8, True, True),
            pytest.param(0.0, True, True),
            pytest.param(1e-8, True, True),
            pytest.param(1e-7, True, True),
            pytest.param(1e-6, True, True),
            pytest.param(1.0, True, True),
            pytest.param(inf, False, False),
            pytest.param(nan, False, True),
        ],
    )
    def test_main(self, *, x: float, expected: bool, expected_nan: bool) -> None:
        assert is_finite_and_non_negative(x, abs_tol=1e-8) is expected
        assert is_finite_and_non_negative_or_nan(x, abs_tol=1e-8) is expected_nan


class TestIsFiniteAndNonPositive:
    @pytest.mark.parametrize(
        ("x", "expected", "expected_nan"),
        [
            pytest.param(-inf, False, False),
            pytest.param(-1.0, True, True),
            pytest.param(-1e-6, True, True),
            pytest.param(-1e-7, True, True),
            pytest.param(-1e-8, True, True),
            pytest.param(0.0, True, True),
            pytest.param(1e-8, True, True),
            pytest.param(1e-7, False, False),
            pytest.param(1e-6, False, False),
            pytest.param(1.0, False, False),
            pytest.param(inf, False, False),
            pytest.param(nan, False, True),
        ],
    )
    def test_main(self, *, x: float, expected: bool, expected_nan: bool) -> None:
        assert is_finite_and_non_positive(x, abs_tol=1e-8) is expected
        assert is_finite_and_non_positive_or_nan(x, abs_tol=1e-8) is expected_nan


class TestIsFiniteAndNonZero:
    @pytest.mark.parametrize(
        ("x", "expected", "expected_nan"),
        [
            pytest.param(-inf, False, False),
            pytest.param(-1.0, True, True),
            pytest.param(-1e-6, True, True),
            pytest.param(-1e-7, True, True),
            pytest.param(-1e-8, False, False),
            pytest.param(0.0, False, False),
            pytest.param(1e-8, False, False),
            pytest.param(1e-7, True, True),
            pytest.param(1e-6, True, True),
            pytest.param(1.0, True, True),
            pytest.param(inf, False, False),
            pytest.param(nan, False, True),
        ],
    )
    def test_main(self, *, x: float, expected: bool, expected_nan: bool) -> None:
        assert is_finite_and_non_zero(x, abs_tol=1e-8) is expected
        assert is_finite_and_non_zero_or_nan(x, abs_tol=1e-8) is expected_nan


class TestIsFiniteAndPositive:
    @pytest.mark.parametrize(
        ("x", "expected", "expected_nan"),
        [
            pytest.param(-inf, False, False),
            pytest.param(-1.0, False, False),
            pytest.param(-1e-6, False, False),
            pytest.param(-1e-7, False, False),
            pytest.param(-1e-8, False, False),
            pytest.param(0.0, False, False),
            pytest.param(1e-8, False, False),
            pytest.param(1e-7, True, True),
            pytest.param(1e-6, True, True),
            pytest.param(1.0, True, True),
            pytest.param(inf, False, False),
            pytest.param(nan, False, True),
        ],
    )
    def test_main(self, *, x: float, expected: bool, expected_nan: bool) -> None:
        assert is_finite_and_positive(x, abs_tol=1e-8) is expected
        assert is_finite_and_positive_or_nan(x, abs_tol=1e-8) is expected_nan


class TestIsGreaterThan:
    @pytest.mark.parametrize(
        ("x", "y", "expected"),
        [
            pytest.param(0.0, -inf, True),
            pytest.param(0.0, -1.0, True),
            pytest.param(0.0, -1e-6, True),
            pytest.param(0.0, -1e-7, True),
            pytest.param(0.0, -1e-8, False),
            pytest.param(0.0, 0.0, False),
            pytest.param(0.0, 1e-8, False),
            pytest.param(0.0, 1e-7, False),
            pytest.param(0.0, 1e-6, False),
            pytest.param(0.0, 1.0, False),
            pytest.param(0.0, inf, False),
            pytest.param(0.0, nan, False),
        ],
    )
    def test_main(self, *, x: float, y: float, expected: bool) -> None:
        assert is_greater_than(x, y, abs_tol=1e-8) is expected

    @pytest.mark.parametrize(
        "y",
        [
            pytest.param(-inf),
            pytest.param(-1.0),
            pytest.param(0.0),
            pytest.param(1.0),
            pytest.param(inf),
            pytest.param(nan),
        ],
    )
    def test_nan(self, *, y: float) -> None:
        assert is_greater_than_or_nan(nan, y)


class TestIsIntegral:
    @pytest.mark.parametrize(
        ("x", "expected", "expected_nan"),
        [
            pytest.param(-inf, True, True),
            pytest.param(-2.0, True, True),
            pytest.param(-1.5, False, False),
            pytest.param(-1.0, True, True),
            pytest.param(-0.5, False, False),
            pytest.param(-1e-6, False, False),
            pytest.param(-1e-7, False, False),
            pytest.param(-1e-8, True, True),
            pytest.param(0.0, True, True),
            pytest.param(1e-8, True, True),
            pytest.param(1e-7, False, False),
            pytest.param(1e-6, False, False),
            pytest.param(0.5, False, False),
            pytest.param(1.0, True, True),
            pytest.param(1.5, False, False),
            pytest.param(2.0, True, True),
            pytest.param(inf, True, True),
            pytest.param(nan, False, True),
        ],
    )
    def test_is_integral(self, *, x: float, expected: bool, expected_nan: bool) -> None:
        assert is_integral(x, abs_tol=1e-8) is expected
        assert is_integral_or_nan(x, abs_tol=1e-8) is expected_nan


class TestIsLessThan:
    @pytest.mark.parametrize(
        ("x", "y", "expected"),
        [
            pytest.param(0.0, -inf, False),
            pytest.param(0.0, -1.0, False),
            pytest.param(0.0, -1e-6, False),
            pytest.param(0.0, -1e-7, False),
            pytest.param(0.0, -1e-8, False),
            pytest.param(0.0, 0.0, False),
            pytest.param(0.0, 1e-8, False),
            pytest.param(0.0, 1e-7, True),
            pytest.param(0.0, 1e-6, True),
            pytest.param(0.0, 1.0, True),
            pytest.param(0.0, inf, True),
            pytest.param(0.0, nan, False),
        ],
    )
    def test_main(self, *, x: float, y: float, expected: bool) -> None:
        assert is_less_than(x, y, abs_tol=1e-8) is expected

    @pytest.mark.parametrize(
        "y",
        [
            pytest.param(-inf),
            pytest.param(-1.0),
            pytest.param(0.0),
            pytest.param(1.0),
            pytest.param(inf),
            pytest.param(nan),
        ],
    )
    def test_nan(self, *, y: float) -> None:
        assert is_less_than_or_nan(nan, y)


class TestIsNegative:
    @pytest.mark.parametrize(
        ("x", "expected", "expected_nan"),
        [
            pytest.param(-inf, True, True),
            pytest.param(-1.0, True, True),
            pytest.param(-1e-6, True, True),
            pytest.param(-1e-7, True, True),
            pytest.param(-1e-8, False, False),
            pytest.param(0.0, False, False),
            pytest.param(1e-8, False, False),
            pytest.param(1e-7, False, False),
            pytest.param(1e-6, False, False),
            pytest.param(1.0, False, False),
            pytest.param(inf, False, False),
            pytest.param(nan, False, True),
        ],
    )
    def test_main(self, *, x: float, expected: bool, expected_nan: bool) -> None:
        assert is_negative(x, abs_tol=1e-8) is expected
        assert is_negative_or_nan(x, abs_tol=1e-8) is expected_nan


class TestIsNonNegative:
    @pytest.mark.parametrize(
        ("x", "expected", "expected_nan"),
        [
            pytest.param(-inf, False, False),
            pytest.param(-1.0, False, False),
            pytest.param(-1e-6, False, False),
            pytest.param(-1e-7, False, False),
            pytest.param(-1e-8, True, True),
            pytest.param(0.0, True, True),
            pytest.param(1e-8, True, True),
            pytest.param(1e-7, True, True),
            pytest.param(1e-6, True, True),
            pytest.param(1.0, True, True),
            pytest.param(inf, True, True),
            pytest.param(nan, False, True),
        ],
    )
    def test_main(self, *, x: float, expected: bool, expected_nan: bool) -> None:
        assert is_non_negative(x, abs_tol=1e-8) is expected
        assert is_non_negative_or_nan(x, abs_tol=1e-8) is expected_nan


class TestIsNonPositive:
    @pytest.mark.parametrize(
        ("x", "expected", "expected_nan"),
        [
            pytest.param(-inf, True, True),
            pytest.param(-1.0, True, True),
            pytest.param(-1e-6, True, True),
            pytest.param(-1e-7, True, True),
            pytest.param(-1e-8, True, True),
            pytest.param(0.0, True, True),
            pytest.param(1e-8, True, True),
            pytest.param(1e-7, False, False),
            pytest.param(1e-6, False, False),
            pytest.param(1.0, False, False),
            pytest.param(inf, False, False),
            pytest.param(nan, False, True),
        ],
    )
    def test_main(self, *, x: float, expected: bool, expected_nan: bool) -> None:
        assert is_non_positive(x, abs_tol=1e-8) is expected
        assert is_non_positive_or_nan(x, abs_tol=1e-8) is expected_nan


class TestIsNonZero:
    @pytest.mark.parametrize(
        ("x", "expected"),
        [
            pytest.param(-inf, True),
            pytest.param(-1.0, True),
            pytest.param(-1e-6, True),
            pytest.param(-1e-7, True),
            pytest.param(-1e-8, False),
            pytest.param(0.0, False),
            pytest.param(1e-8, False),
            pytest.param(1e-7, True),
            pytest.param(1e-6, True),
            pytest.param(1.0, True),
            pytest.param(inf, True),
            pytest.param(nan, True),
        ],
    )
    def test_main(self, *, x: float, expected: bool) -> None:
        assert is_non_zero(x, abs_tol=1e-8) is expected
        assert is_non_zero_or_nan(x, abs_tol=1e-8) is expected


class TestIsPositive:
    @pytest.mark.parametrize(
        ("x", "expected", "expected_nan"),
        [
            pytest.param(-inf, False, False),
            pytest.param(-1.0, False, False),
            pytest.param(-1e-6, False, False),
            pytest.param(-1e-7, False, False),
            pytest.param(-1e-8, False, False),
            pytest.param(0.0, False, False),
            pytest.param(1e-8, False, False),
            pytest.param(1e-7, True, True),
            pytest.param(1e-6, True, True),
            pytest.param(1.0, True, True),
            pytest.param(inf, True, True),
            pytest.param(nan, False, True),
        ],
    )
    def test_main(self, *, x: float, expected: bool, expected_nan: bool) -> None:
        assert is_positive(x, abs_tol=1e-8) is expected
        assert is_positive_or_nan(x, abs_tol=1e-8) is expected_nan


class TestIsZero:
    @pytest.mark.parametrize(
        ("x", "expected", "expected_nan"),
        [
            pytest.param(-inf, False, False),
            pytest.param(-1.0, False, False),
            pytest.param(-1e-6, False, False),
            pytest.param(-1e-7, False, False),
            pytest.param(-1e-8, True, True),
            pytest.param(0.0, True, True),
            pytest.param(1e-8, True, True),
            pytest.param(1e-7, False, False),
            pytest.param(1e-6, False, False),
            pytest.param(1.0, False, False),
            pytest.param(inf, False, False),
            pytest.param(nan, False, True),
        ],
    )
    def test_main(self, *, x: float, expected: bool, expected_nan: bool) -> None:
        assert is_zero(x, abs_tol=1e-8) is expected
        assert is_zero_or_nan(x, abs_tol=1e-8) is expected_nan


class TestIsZeroOrFiniteAndNonMicro:
    @pytest.mark.parametrize(
        ("x", "expected", "expected_nan"),
        [
            pytest.param(-inf, False, False),
            pytest.param(-1.0, True, True),
            pytest.param(-1e-6, True, True),
            pytest.param(-1e-7, True, True),
            pytest.param(-1e-8, False, False),
            pytest.param(0.0, True, True),
            pytest.param(1e-8, False, False),
            pytest.param(1e-7, True, True),
            pytest.param(1e-6, True, True),
            pytest.param(1.0, True, True),
            pytest.param(inf, False, False),
            pytest.param(nan, False, True),
        ],
    )
    def test_main(self, *, x: float, expected: bool, expected_nan: bool) -> None:
        assert is_zero_or_finite_and_non_micro(x, abs_tol=1e-8) is expected
        assert is_zero_or_finite_and_non_micro_or_nan(x, abs_tol=1e-8) is expected_nan


class TestIsZeroOrNonMicro:
    @pytest.mark.parametrize(
        ("x", "expected"),
        [
            pytest.param(-inf, True),
            pytest.param(-1.0, True),
            pytest.param(-1e-6, True),
            pytest.param(-1e-7, True),
            pytest.param(-1e-8, False),
            pytest.param(0.0, True),
            pytest.param(1e-8, False),
            pytest.param(1e-7, True),
            pytest.param(1e-6, True),
            pytest.param(1.0, True),
            pytest.param(inf, True),
            pytest.param(nan, True),
        ],
    )
    def test_main(self, *, x: float, expected: bool) -> None:
        assert is_zero_or_non_micro(x, abs_tol=1e-8) is expected
        assert is_zero_or_non_micro_or_nan(x, abs_tol=1e-8) is expected
