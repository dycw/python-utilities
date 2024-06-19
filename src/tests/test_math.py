from __future__ import annotations

from math import inf, nan

from pytest import mark, param, raises

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
        with raises(CheckIntegerError, match="Integer must be equal to .*; got .*"):
            check_integer(0, equal=1)

    @mark.parametrize("equal_or_approx", [param(10), param((11, 0.1))])
    def test_equal_or_approx_pass(
        self, *, equal_or_approx: int | tuple[int, float]
    ) -> None:
        check_integer(10, equal_or_approx=equal_or_approx)

    @mark.parametrize(
        ("equal_or_approx", "match"),
        [
            param(10, "Integer must be equal to .*; got .*"),
            param(
                (11, 0.1),
                r"Integer must be approximately equal to .* \(error .*\); got .*",
            ),
        ],
    )
    def test_equal_or_approx_fail(
        self, *, equal_or_approx: int | tuple[int, float], match: str
    ) -> None:
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


class TestIsAtLeast:
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
    def test_main(self, *, x: float, y: float, expected: bool) -> None:
        assert is_at_least(x, y, abs_tol=1e-8) is expected

    @mark.parametrize(
        "y", [param(-inf), param(-1.0), param(0.0), param(1.0), param(inf), param(nan)]
    )
    def test_nan(self, *, y: float) -> None:
        assert is_at_least_or_nan(nan, y)


class TestIsAtMost:
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
    def test_main(self, *, x: float, y: float, expected: bool) -> None:
        assert is_at_most(x, y, abs_tol=1e-8) is expected

    @mark.parametrize(
        "y", [param(-inf), param(-1.0), param(0.0), param(1.0), param(inf), param(nan)]
    )
    def test_nan(self, *, y: float) -> None:
        assert is_at_most_or_nan(nan, y)


class TestIsBetween:
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
    def test_main(self, *, x: float, low: float, high: float, expected: bool) -> None:
        assert is_between(x, low, high, abs_tol=1e-8) is expected

    @mark.parametrize(
        "low",
        [param(-inf), param(-1.0), param(0.0), param(1.0), param(inf), param(nan)],
    )
    @mark.parametrize(
        "high",
        [param(-inf), param(-1.0), param(0.0), param(1.0), param(inf), param(nan)],
    )
    def test_nan(self, *, low: float, high: float) -> None:
        assert is_between_or_nan(nan, low, high)


class TestIsEqual:
    @mark.parametrize(
        ("x", "y", "expected"),
        [
            param(0.0, -inf, False),
            param(0.0, -1.0, False),
            param(0.0, -1e-6, False),
            param(0.0, -1e-7, False),
            param(0.0, -1e-8, False),
            param(0.0, 0.0, True),
            param(0.0, 1e-8, False),
            param(0.0, 1e-7, False),
            param(0.0, 1e-6, False),
            param(0.0, 1.0, False),
            param(0.0, inf, False),
            param(0.0, nan, False),
        ],
    )
    def test_main(self, *, x: float, y: float, expected: bool) -> None:
        assert is_equal(x, y) is expected
        assert is_equal(y, x) is expected


class TestIsEqualOrApprox:
    @mark.parametrize(
        ("x", "y", "expected"),
        [
            param(0, 0, True),
            param(0, 1, False),
            param(1, 0, False),
            param(10, (8, 0.1), False),
            param(10, (9, 0.1), True),
            param(10, (10, 0.1), True),
            param(10, (11, 0.1), True),
            param(10, (12, 0.1), False),
            param((10, 0.1), (8, 0.1), False),
            param((10, 0.1), (9, 0.1), True),
            param((10, 0.1), (10, 0.1), True),
            param((10, 0.1), (11, 0.1), True),
            param((10, 0.1), (12, 0.1), False),
        ],
    )
    def test_main(
        self, *, x: int | tuple[int, float], y: int | tuple[int, float], expected: bool
    ) -> None:
        assert is_equal_or_approx(x, y) is expected
        assert is_equal_or_approx(y, x) is expected


class TestIsFinite:
    @mark.parametrize(
        ("x", "expected", "expected_nan"),
        [
            param(-inf, False, False),
            param(-1.0, True, True),
            param(0.0, True, True),
            param(1.0, True, True),
            param(inf, False, False),
            param(nan, False, True),
        ],
    )
    def test_main(self, *, x: float, expected: bool, expected_nan: bool) -> None:
        assert is_finite(x) is expected
        assert is_finite_or_nan(x) is expected_nan


class TestIsFiniteAndIntegral:
    @mark.parametrize(
        ("x", "expected", "expected_nan"),
        [
            param(-inf, False, False),
            param(-2.0, True, True),
            param(-1.5, False, False),
            param(-1.0, True, True),
            param(-0.5, False, False),
            param(-1e-6, False, False),
            param(-1e-7, False, False),
            param(-1e-8, True, True),
            param(0.0, True, True),
            param(1e-8, True, True),
            param(1e-7, False, False),
            param(1e-6, False, False),
            param(0.5, False, False),
            param(1.0, True, True),
            param(1.5, False, False),
            param(2.0, True, True),
            param(inf, False, False),
            param(nan, False, True),
        ],
    )
    def test_main(self, *, x: float, expected: bool, expected_nan: bool) -> None:
        assert is_finite_and_integral(x, abs_tol=1e-8) is expected
        assert is_finite_and_integral_or_nan(x, abs_tol=1e-8) is expected_nan


class TestIsFiniteAndNegative:
    @mark.parametrize(
        ("x", "expected", "expected_nan"),
        [
            param(-inf, False, False),
            param(-1.0, True, True),
            param(-1e-6, True, True),
            param(-1e-7, True, True),
            param(-1e-8, False, False),
            param(0.0, False, False),
            param(1e-8, False, False),
            param(1e-7, False, False),
            param(1e-6, False, False),
            param(1.0, False, False),
            param(inf, False, False),
            param(nan, False, True),
        ],
    )
    def test_main(self, *, x: float, expected: bool, expected_nan: bool) -> None:
        assert is_finite_and_negative(x, abs_tol=1e-8) is expected
        assert is_finite_and_negative_or_nan(x, abs_tol=1e-8) is expected_nan


class TestIsFiniteAndNonNegative:
    @mark.parametrize(
        ("x", "expected", "expected_nan"),
        [
            param(-inf, False, False),
            param(-1.0, False, False),
            param(-1e-6, False, False),
            param(-1e-7, False, False),
            param(-1e-8, True, True),
            param(0.0, True, True),
            param(1e-8, True, True),
            param(1e-7, True, True),
            param(1e-6, True, True),
            param(1.0, True, True),
            param(inf, False, False),
            param(nan, False, True),
        ],
    )
    def test_main(self, *, x: float, expected: bool, expected_nan: bool) -> None:
        assert is_finite_and_non_negative(x, abs_tol=1e-8) is expected
        assert is_finite_and_non_negative_or_nan(x, abs_tol=1e-8) is expected_nan


class TestIsFiniteAndNonPositive:
    @mark.parametrize(
        ("x", "expected", "expected_nan"),
        [
            param(-inf, False, False),
            param(-1.0, True, True),
            param(-1e-6, True, True),
            param(-1e-7, True, True),
            param(-1e-8, True, True),
            param(0.0, True, True),
            param(1e-8, True, True),
            param(1e-7, False, False),
            param(1e-6, False, False),
            param(1.0, False, False),
            param(inf, False, False),
            param(nan, False, True),
        ],
    )
    def test_main(self, *, x: float, expected: bool, expected_nan: bool) -> None:
        assert is_finite_and_non_positive(x, abs_tol=1e-8) is expected
        assert is_finite_and_non_positive_or_nan(x, abs_tol=1e-8) is expected_nan


class TestIsFiniteAndNonZero:
    @mark.parametrize(
        ("x", "expected", "expected_nan"),
        [
            param(-inf, False, False),
            param(-1.0, True, True),
            param(-1e-6, True, True),
            param(-1e-7, True, True),
            param(-1e-8, False, False),
            param(0.0, False, False),
            param(1e-8, False, False),
            param(1e-7, True, True),
            param(1e-6, True, True),
            param(1.0, True, True),
            param(inf, False, False),
            param(nan, False, True),
        ],
    )
    def test_main(self, *, x: float, expected: bool, expected_nan: bool) -> None:
        assert is_finite_and_non_zero(x, abs_tol=1e-8) is expected
        assert is_finite_and_non_zero_or_nan(x, abs_tol=1e-8) is expected_nan


class TestIsFiniteAndPositive:
    @mark.parametrize(
        ("x", "expected", "expected_nan"),
        [
            param(-inf, False, False),
            param(-1.0, False, False),
            param(-1e-6, False, False),
            param(-1e-7, False, False),
            param(-1e-8, False, False),
            param(0.0, False, False),
            param(1e-8, False, False),
            param(1e-7, True, True),
            param(1e-6, True, True),
            param(1.0, True, True),
            param(inf, False, False),
            param(nan, False, True),
        ],
    )
    def test_main(self, *, x: float, expected: bool, expected_nan: bool) -> None:
        assert is_finite_and_positive(x, abs_tol=1e-8) is expected
        assert is_finite_and_positive_or_nan(x, abs_tol=1e-8) is expected_nan


class TestIsGreaterThan:
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
    def test_main(self, *, x: float, y: float, expected: bool) -> None:
        assert is_greater_than(x, y, abs_tol=1e-8) is expected

    @mark.parametrize(
        "y", [param(-inf), param(-1.0), param(0.0), param(1.0), param(inf), param(nan)]
    )
    def test_nan(self, *, y: float) -> None:
        assert is_greater_than_or_nan(nan, y)


class TestIsIntegral:
    @mark.parametrize(
        ("x", "expected", "expected_nan"),
        [
            param(-inf, True, True),
            param(-2.0, True, True),
            param(-1.5, False, False),
            param(-1.0, True, True),
            param(-0.5, False, False),
            param(-1e-6, False, False),
            param(-1e-7, False, False),
            param(-1e-8, True, True),
            param(0.0, True, True),
            param(1e-8, True, True),
            param(1e-7, False, False),
            param(1e-6, False, False),
            param(0.5, False, False),
            param(1.0, True, True),
            param(1.5, False, False),
            param(2.0, True, True),
            param(inf, True, True),
            param(nan, False, True),
        ],
    )
    def test_is_integral(self, *, x: float, expected: bool, expected_nan: bool) -> None:
        assert is_integral(x, abs_tol=1e-8) is expected
        assert is_integral_or_nan(x, abs_tol=1e-8) is expected_nan


class TestIsLessThan:
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
    def test_main(self, *, x: float, y: float, expected: bool) -> None:
        assert is_less_than(x, y, abs_tol=1e-8) is expected

    @mark.parametrize(
        "y", [param(-inf), param(-1.0), param(0.0), param(1.0), param(inf), param(nan)]
    )
    def test_nan(self, *, y: float) -> None:
        assert is_less_than_or_nan(nan, y)


class TestIsNegative:
    @mark.parametrize(
        ("x", "expected", "expected_nan"),
        [
            param(-inf, True, True),
            param(-1.0, True, True),
            param(-1e-6, True, True),
            param(-1e-7, True, True),
            param(-1e-8, False, False),
            param(0.0, False, False),
            param(1e-8, False, False),
            param(1e-7, False, False),
            param(1e-6, False, False),
            param(1.0, False, False),
            param(inf, False, False),
            param(nan, False, True),
        ],
    )
    def test_main(self, *, x: float, expected: bool, expected_nan: bool) -> None:
        assert is_negative(x, abs_tol=1e-8) is expected
        assert is_negative_or_nan(x, abs_tol=1e-8) is expected_nan


class TestIsNonNegative:
    @mark.parametrize(
        ("x", "expected", "expected_nan"),
        [
            param(-inf, False, False),
            param(-1.0, False, False),
            param(-1e-6, False, False),
            param(-1e-7, False, False),
            param(-1e-8, True, True),
            param(0.0, True, True),
            param(1e-8, True, True),
            param(1e-7, True, True),
            param(1e-6, True, True),
            param(1.0, True, True),
            param(inf, True, True),
            param(nan, False, True),
        ],
    )
    def test_main(self, *, x: float, expected: bool, expected_nan: bool) -> None:
        assert is_non_negative(x, abs_tol=1e-8) is expected
        assert is_non_negative_or_nan(x, abs_tol=1e-8) is expected_nan


class TestIsNonPositive:
    @mark.parametrize(
        ("x", "expected", "expected_nan"),
        [
            param(-inf, True, True),
            param(-1.0, True, True),
            param(-1e-6, True, True),
            param(-1e-7, True, True),
            param(-1e-8, True, True),
            param(0.0, True, True),
            param(1e-8, True, True),
            param(1e-7, False, False),
            param(1e-6, False, False),
            param(1.0, False, False),
            param(inf, False, False),
            param(nan, False, True),
        ],
    )
    def test_main(self, *, x: float, expected: bool, expected_nan: bool) -> None:
        assert is_non_positive(x, abs_tol=1e-8) is expected
        assert is_non_positive_or_nan(x, abs_tol=1e-8) is expected_nan


class TestIsNonZero:
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
    def test_main(self, *, x: float, expected: bool) -> None:
        assert is_non_zero(x, abs_tol=1e-8) is expected
        assert is_non_zero_or_nan(x, abs_tol=1e-8) is expected


class TestIsPositive:
    @mark.parametrize(
        ("x", "expected", "expected_nan"),
        [
            param(-inf, False, False),
            param(-1.0, False, False),
            param(-1e-6, False, False),
            param(-1e-7, False, False),
            param(-1e-8, False, False),
            param(0.0, False, False),
            param(1e-8, False, False),
            param(1e-7, True, True),
            param(1e-6, True, True),
            param(1.0, True, True),
            param(inf, True, True),
            param(nan, False, True),
        ],
    )
    def test_main(self, *, x: float, expected: bool, expected_nan: bool) -> None:
        assert is_positive(x, abs_tol=1e-8) is expected
        assert is_positive_or_nan(x, abs_tol=1e-8) is expected_nan


class TestIsZero:
    @mark.parametrize(
        ("x", "expected", "expected_nan"),
        [
            param(-inf, False, False),
            param(-1.0, False, False),
            param(-1e-6, False, False),
            param(-1e-7, False, False),
            param(-1e-8, True, True),
            param(0.0, True, True),
            param(1e-8, True, True),
            param(1e-7, False, False),
            param(1e-6, False, False),
            param(1.0, False, False),
            param(inf, False, False),
            param(nan, False, True),
        ],
    )
    def test_main(self, *, x: float, expected: bool, expected_nan: bool) -> None:
        assert is_zero(x, abs_tol=1e-8) is expected
        assert is_zero_or_nan(x, abs_tol=1e-8) is expected_nan


class TestIsZeroOrFiniteAndNonMicro:
    @mark.parametrize(
        ("x", "expected", "expected_nan"),
        [
            param(-inf, False, False),
            param(-1.0, True, True),
            param(-1e-6, True, True),
            param(-1e-7, True, True),
            param(-1e-8, False, False),
            param(0.0, True, True),
            param(1e-8, False, False),
            param(1e-7, True, True),
            param(1e-6, True, True),
            param(1.0, True, True),
            param(inf, False, False),
            param(nan, False, True),
        ],
    )
    def test_main(self, *, x: float, expected: bool, expected_nan: bool) -> None:
        assert is_zero_or_finite_and_non_micro(x, abs_tol=1e-8) is expected
        assert is_zero_or_finite_and_non_micro_or_nan(x, abs_tol=1e-8) is expected_nan


class TestIsZeroOrNonMicro:
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
    def test_main(self, *, x: float, expected: bool) -> None:
        assert is_zero_or_non_micro(x, abs_tol=1e-8) is expected
        assert is_zero_or_non_micro_or_nan(x, abs_tol=1e-8) is expected
