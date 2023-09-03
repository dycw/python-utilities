from __future__ import annotations

from numpy import inf, nan
from pytest import mark, param

from utilities.numpy.checks import (
    is_at_least,
    is_at_least_or_nan,
    is_at_most,
    is_at_most_or_nan,
    is_between,
    is_between_or_nan,
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


class TestIsAtLeast:
    @mark.parametrize(
        ("x", "y", "equal_nan", "expected"),
        [
            param(0.0, -inf, False, True),
            param(0.0, -1.0, False, True),
            param(0.0, -1e-6, False, True),
            param(0.0, -1e-7, False, True),
            param(0.0, -1e-8, False, True),
            param(0.0, 0.0, False, True),
            param(0.0, 1e-8, False, True),
            param(0.0, 1e-7, False, False),
            param(0.0, 1e-6, False, False),
            param(0.0, 1.0, False, False),
            param(0.0, inf, False, False),
            param(0.0, nan, False, False),
            param(nan, nan, True, True),
        ],
    )
    def test_main(
        self, *, x: float, y: float, equal_nan: bool, expected: bool
    ) -> None:
        assert is_at_least(x, y, equal_nan=equal_nan).item() is expected

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
    def test_nan(self, y: float) -> None:
        assert is_at_least_or_nan(nan, y)


class TestIsAtMost:
    @mark.parametrize(
        ("x", "y", "equal_nan", "expected"),
        [
            param(0.0, -inf, False, False),
            param(0.0, -1.0, False, False),
            param(0.0, -1e-6, False, False),
            param(0.0, -1e-7, False, False),
            param(0.0, -1e-8, False, True),
            param(0.0, 0.0, False, True),
            param(0.0, 1e-8, False, True),
            param(0.0, 1e-7, False, True),
            param(0.0, 1e-6, False, True),
            param(0.0, 1.0, False, True),
            param(0.0, inf, False, True),
            param(0.0, nan, False, False),
            param(nan, nan, True, True),
        ],
    )
    def test_main(
        self, *, x: float, y: float, equal_nan: bool, expected: bool
    ) -> None:
        assert is_at_most(x, y, equal_nan=equal_nan).item() is expected

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
    def test_nan(self, y: float) -> None:
        assert is_at_most_or_nan(nan, y)


class TestIsBetween:
    @mark.parametrize(
        ("x", "low", "high", "equal_nan", "expected"),
        [
            param(0.0, -1.0, -1.0, False, False),
            param(0.0, -1.0, 0.0, False, True),
            param(0.0, -1.0, 1.0, False, True),
            param(0.0, 0.0, -1.0, False, False),
            param(0.0, 0.0, 0.0, False, True),
            param(0.0, 0.0, 1.0, False, True),
            param(0.0, 1.0, -1.0, False, False),
            param(0.0, 1.0, 0.0, False, False),
            param(0.0, 1.0, 1.0, False, False),
            param(nan, -1.0, 1.0, False, False),
        ],
    )
    def test_main(
        self,
        *,
        x: float,
        low: float,
        high: float,
        equal_nan: bool,
        expected: bool,
    ) -> None:
        assert is_between(x, low, high, equal_nan=equal_nan).item() is expected

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
    def test_nan(self, low: float, high: float) -> None:
        assert is_between_or_nan(nan, low, high)


class TestIsFiniteAndIntegral:
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
    def test_main(self, *, x: float, expected: bool) -> None:
        assert is_finite_and_integral(x).item() is expected

    def test_nan(self) -> None:
        assert is_finite_and_integral_or_nan(nan)


class TestIsFiniteOrNan:
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
    def test_main(self, *, x: float, expected: bool) -> None:
        assert is_finite_or_nan(x).item() is expected


class TestIsFiniteAndNegative:
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
    def test_main(self, *, x: float, expected: bool) -> None:
        assert is_finite_and_negative(x).item() is expected

    def test_nan(self) -> None:
        assert is_finite_and_negative_or_nan(nan)


class TestIsFiniteAndNonNegative:
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
    def test_main(self, *, x: float, expected: bool) -> None:
        assert is_finite_and_non_negative(x).item() is expected

    def test_nan(self) -> None:
        assert is_finite_and_non_negative_or_nan(nan)


class TestIsFiniteAndNonPositive:
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
    def test_main(self, *, x: float, expected: bool) -> None:
        assert is_finite_and_non_positive(x).item() is expected

    def test_nan(self) -> None:
        assert is_finite_and_non_positive_or_nan(nan)


class TestIsFiniteAndNonZero:
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
    def test_main(self, *, x: float, expected: bool) -> None:
        assert is_finite_and_non_zero(x).item() is expected

    def test_nan(self) -> None:
        assert is_finite_and_non_zero_or_nan(nan)


class TestIsFiniteAndPositive:
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
    def test_main(self, *, x: float, expected: bool) -> None:
        assert is_finite_and_positive(x).item() is expected

    def test_nan(self) -> None:
        assert is_finite_and_positive_or_nan(nan)


class TestIsGreaterThan:
    @mark.parametrize(
        ("x", "y", "equal_nan", "expected"),
        [
            param(0.0, -inf, False, True),
            param(0.0, -1.0, False, True),
            param(0.0, -1e-6, False, True),
            param(0.0, -1e-7, False, True),
            param(0.0, -1e-8, False, False),
            param(0.0, 0.0, False, False),
            param(0.0, 1e-8, False, False),
            param(0.0, 1e-7, False, False),
            param(0.0, 1e-6, False, False),
            param(0.0, 1.0, False, False),
            param(0.0, inf, False, False),
            param(0.0, nan, False, False),
            param(nan, nan, True, True),
        ],
    )
    def test_main(
        self, *, x: float, y: float, equal_nan: bool, expected: bool
    ) -> None:
        assert is_greater_than(x, y, equal_nan=equal_nan).item() is expected

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
    def test_nan(self, y: float) -> None:
        assert is_greater_than_or_nan(nan, y)


class TestIsIntegral:
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
    def test_main(self, *, x: float, expected: bool) -> None:
        assert is_integral(x).item() is expected

    def test_nan(self) -> None:
        assert is_integral_or_nan(nan)


class TestIsLessThan:
    @mark.parametrize(
        ("x", "y", "equal_nan", "expected"),
        [
            param(0.0, -inf, False, False),
            param(0.0, -1.0, False, False),
            param(0.0, -1e-6, False, False),
            param(0.0, -1e-7, False, False),
            param(0.0, -1e-8, False, False),
            param(0.0, 0.0, False, False),
            param(0.0, 1e-8, False, False),
            param(0.0, 1e-7, False, True),
            param(0.0, 1e-6, False, True),
            param(0.0, 1.0, False, True),
            param(0.0, inf, False, True),
            param(0.0, nan, False, False),
            param(nan, nan, True, True),
        ],
    )
    def test_main(
        self, *, x: float, y: float, equal_nan: bool, expected: bool
    ) -> None:
        assert is_less_than(x, y, equal_nan=equal_nan).item() is expected

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
    def test_nan(self, y: float) -> None:
        assert is_less_than_or_nan(nan, y)


class TestIsNegative:
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
    def test_main(self, *, x: float, expected: bool) -> None:
        assert is_negative(x).item() is expected

    def test_nan(self) -> None:
        assert is_negative_or_nan(nan)


class TestIsNonNegative:
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
    def test_main(self, *, x: float, expected: bool) -> None:
        assert is_non_negative(x).item() is expected

    def test_nan(self) -> None:
        assert is_non_negative_or_nan(nan)


class TestIsNonPositive:
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
    def test_main(self, *, x: float, expected: bool) -> None:
        assert is_non_positive(x).item() is expected

    def test_nan(self) -> None:
        assert is_non_positive_or_nan(nan)


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
        assert is_non_zero(x).item() is expected

    def test_nan(self) -> None:
        assert is_non_zero_or_nan(nan)


class TestIsPositive:
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
    def test_main(self, *, x: float, expected: bool) -> None:
        assert is_positive(x).item() is expected

    def test_nan(self) -> None:
        assert is_positive_or_nan(nan)


class TestIsZero:
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
    def test_main(self, *, x: float, expected: bool) -> None:
        assert is_zero(x).item() is expected

    def test_is_zero_or_nan(self) -> None:
        assert is_zero_or_nan(nan)


class TestIsZeroOrFiniteAndMicro:
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
    def test_main(self, *, x: float, expected: bool) -> None:
        assert is_zero_or_finite_and_non_micro(x).item() is expected

    def test_nan(self) -> None:
        assert is_zero_or_finite_and_non_micro_or_nan(nan)


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
        assert is_zero_or_non_micro(x).item() is expected

    def test_nan(self) -> None:
        assert is_zero_or_non_micro_or_nan(nan)
