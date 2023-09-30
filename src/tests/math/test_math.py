from __future__ import annotations

from numpy import inf
from numpy import nan
from pytest import mark
from pytest import param

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
    def test_is_at_least_or_nan(self, y: float) -> None:
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
    def test_is_at_most_or_nan(self, y: float) -> None:
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
    def test_is_between_or_nan(self, low: float, high: float) -> None:
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
    def test_is_greater_than_or_nan(self, y: float) -> None:
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
    def test_is_less_than_or_nan(self, y: float) -> None:
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
