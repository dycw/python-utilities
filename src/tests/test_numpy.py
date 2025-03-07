from __future__ import annotations

from re import escape
from typing import TYPE_CHECKING, Any, Literal

from hypothesis import assume, given
from hypothesis.strategies import DataObject, data, floats, integers, none
from numpy import (
    arange,
    array,
    eye,
    full,
    inf,
    isclose,
    median,
    nan,
    ndarray,
    ones,
    zeros,
    zeros_like,
)
from numpy.random import Generator
from numpy.testing import assert_equal
from pytest import mark, param, raises

from utilities.hypothesis import float_arrays
from utilities.numpy import (
    DEFAULT_RNG,
    AsIntError,
    FlatN0EmptyError,
    FlatN0MultipleError,
    NDArrayF,
    NDArrayI,
    ShiftError,
    array_indexer,
    as_int,
    discretize,
    fillna,
    flatn0,
    has_dtype,
    is_at_least,
    is_at_least_or_nan,
    is_at_most,
    is_at_most_or_nan,
    is_between,
    is_between_or_nan,
    is_empty,
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
    is_non_empty,
    is_non_negative,
    is_non_negative_or_nan,
    is_non_positive,
    is_non_positive_or_nan,
    is_non_singular,
    is_non_zero,
    is_non_zero_or_nan,
    is_positive,
    is_positive_or_nan,
    is_positive_semidefinite,
    is_symmetric,
    is_zero,
    is_zero_or_finite_and_non_micro,
    is_zero_or_finite_and_non_micro_or_nan,
    is_zero_or_nan,
    is_zero_or_non_micro,
    is_zero_or_non_micro_or_nan,
    maximum,
    minimum,
    shift,
    shift_bool,
)

if TYPE_CHECKING:
    from collections.abc import Sequence


class TestArrayIndexer:
    @mark.parametrize(
        ("i", "ndim", "expected"),
        [
            param(0, 1, (0,)),
            param(0, 2, (slice(None), 0)),
            param(1, 2, (slice(None), 1)),
            param(0, 3, (slice(None), slice(None), 0)),
            param(1, 3, (slice(None), slice(None), 1)),
            param(2, 3, (slice(None), slice(None), 2)),
        ],
    )
    def test_main(
        self, *, i: int, ndim: int, expected: tuple[int | slice, ...]
    ) -> None:
        assert array_indexer(i, ndim) == expected

    @mark.parametrize(
        ("i", "ndim", "axis", "expected"),
        [
            param(0, 1, 0, (0,)),
            param(0, 2, 0, (0, slice(None))),
            param(0, 2, 1, (slice(None), 0)),
            param(1, 2, 0, (1, slice(None))),
            param(1, 2, 1, (slice(None), 1)),
            param(0, 3, 0, (0, slice(None), slice(None))),
            param(0, 3, 1, (slice(None), 0, slice(None))),
            param(0, 3, 2, (slice(None), slice(None), 0)),
            param(1, 3, 0, (1, slice(None), slice(None))),
            param(1, 3, 1, (slice(None), 1, slice(None))),
            param(1, 3, 2, (slice(None), slice(None), 1)),
            param(2, 3, 0, (2, slice(None), slice(None))),
            param(2, 3, 1, (slice(None), 2, slice(None))),
            param(2, 3, 2, (slice(None), slice(None), 2)),
        ],
    )
    def test_axis(
        self, *, i: int, ndim: int, axis: int, expected: tuple[int | slice, ...]
    ) -> None:
        assert array_indexer(i, ndim, axis=axis) == expected


class TestAsInt:
    @given(n=integers(-10, 10), fuzz=floats(-1e-8, 1e-8) | none())
    def test_main(self, *, n: int, fuzz: float | None) -> None:
        n_use = n if fuzz is None else (n + fuzz)
        arr = array([n_use], dtype=float)
        result = as_int(arr)
        expected = array([n], dtype=int)
        assert_equal(result, expected)

    @given(n=integers(-10, 10))
    def test_nan_elements_filled(self, *, n: int) -> None:
        arr = array([nan], dtype=float)
        result = as_int(arr, nan=n)
        expected = array([n], dtype=int)
        assert_equal(result, expected)

    @given(n=integers(-10, 10))
    def test_inf_elements_filled(self, *, n: int) -> None:
        arr = array([inf], dtype=float)
        result = as_int(arr, inf=n)
        expected = array([n], dtype=int)
        assert_equal(result, expected)

    @mark.parametrize("value", [param(inf), param(nan), param(0.5)])
    def test_errors(self, *, value: float) -> None:
        arr = array([value], dtype=float)
        with raises(AsIntError):
            _ = as_int(arr)


class TestDefaultRng:
    def test_main(self) -> None:
        assert isinstance(DEFAULT_RNG, Generator)


class TestDiscretize:
    @given(arr=float_arrays(shape=integers(0, 10), min_value=-1.0, max_value=1.0))
    def test_1_bin(self, *, arr: NDArrayF) -> None:
        result = discretize(arr, 1)
        expected = zeros_like(arr, dtype=float)
        assert_equal(result, expected)

    @given(
        arr=float_arrays(
            shape=integers(1, 10), min_value=-1.0, max_value=1.0, unique=True
        )
    )
    def test_2_bins(self, *, arr: NDArrayF) -> None:
        _ = assume(len(arr) % 2 == 0)
        result = discretize(arr, 2)
        med = median(arr)
        is_below = (arr < med) & ~isclose(arr, med)
        assert isclose(result[is_below], 0.0).all()
        is_above = (arr > med) & ~isclose(arr, med)
        assert isclose(result[is_above], 1.0).all()

    @given(bins=integers(1, 10))
    def test_empty(self, *, bins: int) -> None:
        arr = array([], dtype=float)
        result = discretize(arr, bins)
        assert_equal(result, arr)

    @given(n=integers(0, 10), bins=integers(1, 10))
    def test_all_nan(self, *, n: int, bins: int) -> None:
        arr = full(n, nan, dtype=float)
        result = discretize(arr, bins)
        assert_equal(result, arr)

    @mark.parametrize(
        ("arr_v", "bins", "expected_v"),
        [
            param(
                [1.0, 2.0, 3.0, 4.0],
                [0.0, 0.25, 0.5, 0.75, 1.0],
                [0.0, 1.0, 2.0, 3.0],
                id="equally spaced",
            ),
            param(
                [1.0, 2.0, 3.0, 4.0],
                [0.0, 0.1, 0.9, 1.0],
                [0.0, 1.0, 1.0, 2.0],
                id="unequally spaced",
            ),
            param(
                [1.0, 2.0, 3.0],
                [0.0, 0.33, 1.0],
                [0.0, 1.0, 1.0],
                id="equally spaced 1 to 2",
            ),
            param(
                [1.0, 2.0, 3.0, nan],
                [0.0, 0.33, 1.0],
                [0.0, 1.0, 1.0, nan],
                id="with nan",
            ),
        ],
    )
    def test_bins_of_floats(
        self,
        *,
        arr_v: Sequence[float],
        bins: Sequence[float],
        expected_v: Sequence[float],
    ) -> None:
        arr = array(arr_v, dtype=float)
        result = discretize(arr, bins)
        expected = array(expected_v, dtype=float)
        assert_equal(result, expected)


class TestFillNa:
    @mark.parametrize(
        ("init", "value", "expected_v"),
        [
            param(0.0, 0.0, 0.0),
            param(0.0, nan, 0.0),
            param(0.0, inf, 0.0),
            param(nan, 0.0, 0.0),
            param(nan, nan, nan),
            param(nan, inf, inf),
            param(inf, 0.0, inf),
            param(inf, nan, inf),
            param(inf, inf, inf),
        ],
    )
    def test_main(self, *, init: float, value: float, expected_v: float) -> None:
        arr = array([init], dtype=float)
        result = fillna(arr, value=value)
        expected = array([expected_v], dtype=float)
        assert_equal(result, expected)


class TestFlatN0:
    @given(data=data(), n=integers(1, 10))
    def test_main(self, *, data: DataObject, n: int) -> None:
        i = data.draw(integers(0, n - 1))
        arr = arange(n) == i
        result = flatn0(arr)
        assert result == i

    def test_empty_error(self) -> None:
        with raises(FlatN0EmptyError, match=escape(r"Array [] must contain a True.")):
            _ = flatn0(zeros(0, dtype=bool))

    def test_multiple_error(self) -> None:
        with raises(
            FlatN0MultipleError,
            match=escape("Array [ True  True] must contain at most one True."),
        ):
            _ = flatn0(ones(2, dtype=bool))


class TestHasDtype:
    @mark.parametrize(("dtype", "expected"), [param(float, True), param(int, False)])
    @mark.parametrize("is_tuple", [param(True), param(False)])
    def test_main(self, *, dtype: Any, is_tuple: bool, expected: bool) -> None:
        against = (dtype,) if is_tuple else dtype
        result = has_dtype(array([], dtype=float), against)
        assert result is expected


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
    def test_main(self, *, x: float, y: float, equal_nan: bool, expected: bool) -> None:
        assert is_at_least(x, y, equal_nan=equal_nan).item() is expected

    @mark.parametrize(
        "y", [param(-inf), param(-1.0), param(0.0), param(1.0), param(inf), param(nan)]
    )
    def test_nan(self, *, y: float) -> None:
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
    def test_main(self, *, x: float, y: float, equal_nan: bool, expected: bool) -> None:
        assert is_at_most(x, y, equal_nan=equal_nan).item() is expected

    @mark.parametrize(
        "y", [param(-inf), param(-1.0), param(0.0), param(1.0), param(inf), param(nan)]
    )
    def test_nan(self, *, y: float) -> None:
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
        self, *, x: float, low: float, high: float, equal_nan: bool, expected: bool
    ) -> None:
        assert is_between(x, low, high, equal_nan=equal_nan).item() is expected

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


class TestIsEmptyAndIsNotEmpty:
    @mark.parametrize(
        ("shape", "expected"),
        [
            param(0, "empty"),
            param(1, "non-empty"),
            param(2, "non-empty"),
            param((), "empty"),
            param((0,), "empty"),
            param((1,), "non-empty"),
            param((2,), "non-empty"),
            param((0, 0), "empty"),
            param((0, 1), "empty"),
            param((0, 2), "empty"),
            param((1, 0), "empty"),
            param((1, 1), "non-empty"),
            param((1, 2), "non-empty"),
            param((2, 0), "empty"),
            param((2, 1), "non-empty"),
            param((2, 2), "non-empty"),
        ],
    )
    @mark.parametrize("kind", [param("shape"), param("array")])
    def test_main(
        self,
        *,
        shape: int | tuple[int, ...],
        kind: Literal["shape", "array"],
        expected: Literal["empty", "non-empty"],
    ) -> None:
        shape_or_array = shape if kind == "shape" else zeros(shape, dtype=float)
        assert is_empty(shape_or_array) is (expected == "empty")
        assert is_non_empty(shape_or_array) is (expected == "non-empty")


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
    def test_main(self, *, x: float, y: float, equal_nan: bool, expected: bool) -> None:
        assert is_greater_than(x, y, equal_nan=equal_nan).item() is expected

    @mark.parametrize(
        "y", [param(-inf), param(-1.0), param(0.0), param(1.0), param(inf), param(nan)]
    )
    def test_nan(self, *, y: float) -> None:
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
    def test_main(self, *, x: float, y: float, equal_nan: bool, expected: bool) -> None:
        assert is_less_than(x, y, equal_nan=equal_nan).item() is expected

    @mark.parametrize(
        "y", [param(-inf), param(-1.0), param(0.0), param(1.0), param(inf), param(nan)]
    )
    def test_nan(self, *, y: float) -> None:
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


class TestIsNonSingular:
    @mark.parametrize(
        ("array", "expected"), [param(eye(2), True), param(ones((2, 2)), False)]
    )
    @mark.parametrize("dtype", [param(float), param(int)])
    def test_main(self, *, array: NDArrayF, dtype: Any, expected: bool) -> None:
        assert is_non_singular(array.astype(dtype)) is expected

    def test_overflow(self) -> None:
        arr = array([[0.0, 0.0], [5e-323, 0.0]], dtype=float)
        assert not is_non_singular(arr)


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


class TestIsPositiveSemiDefinite:
    @mark.parametrize(
        ("array", "expected"),
        [
            param(eye(2), True),
            param(zeros((1, 2), dtype=float), False),
            param(arange(4).reshape((2, 2)), False),
        ],
    )
    @mark.parametrize("dtype", [param(float), param(int)])
    def test_main(
        self, *, array: NDArrayF | NDArrayI, dtype: Any, expected: bool
    ) -> None:
        assert is_positive_semidefinite(array.astype(dtype)) is expected

    @given(array=float_arrays(shape=(2, 2), min_value=-1.0, max_value=1.0))
    def test_overflow(self, *, array: NDArrayF) -> None:
        _ = is_positive_semidefinite(array)


class TestIsSymmetric:
    @mark.parametrize(
        ("array", "expected"),
        [
            param(eye(2), True),
            param(zeros((1, 2), dtype=float), False),
            param(arange(4).reshape((2, 2)), False),
        ],
    )
    @mark.parametrize("dtype", [param(float), param(int)])
    def test_main(
        self, *, array: NDArrayF | NDArrayI, dtype: Any, expected: bool
    ) -> None:
        assert is_symmetric(array.astype(dtype)) is expected


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


class TestMaximumMinimum:
    def test_maximum_floats(self) -> None:
        result = maximum(1.0, 2.0)
        assert isinstance(result, float)

    def test_maximum_arrays(self) -> None:
        result = maximum(array([1.0], dtype=float), array([2.0], dtype=float))
        assert isinstance(result, ndarray)

    def test_minimum_floats(self) -> None:
        result = minimum(1.0, 2.0)
        assert isinstance(result, float)

    def test_minimum_arrays(self) -> None:
        result = minimum(array([1.0], dtype=float), array([2.0], dtype=float))
        assert isinstance(result, ndarray)


class TestShift:
    @mark.parametrize(
        ("n", "expected_v"),
        [
            param(1, [nan, 0.0, 1.0]),
            param(2, [nan, nan, 0.0]),
            param(-1, [1.0, 2.0, nan]),
            param(-2, [2.0, nan, nan]),
        ],
    )
    @mark.parametrize("dtype", [param(float), param(int)])
    def test_1d(self, *, n: int, expected_v: Sequence[float], dtype: type[Any]) -> None:
        arr = arange(3, dtype=dtype)
        result = shift(arr, n=n)
        expected = array(expected_v, dtype=float)
        assert_equal(result, expected)

    @mark.parametrize(
        ("axis", "n", "expected_v"),
        [
            param(
                0,
                1,
                [4 * [nan], [0.0, 1.0, 2.0, 3.0], [4.0, 5.0, 6.0, 7.0]],
                id="axis=0, n=1",
            ),
            param(0, 2, [4 * [nan], 4 * [nan], [0.0, 1.0, 2.0, 3.0]], id="axis=0, n=2"),
            param(
                0,
                -1,
                [[4.0, 5.0, 6.0, 7.0], [8.0, 9.0, 10.0, 11.0], 4 * [nan]],
                id="axis=0, n=-1",
            ),
            param(
                0, -2, [[8.0, 9.0, 10.0, 11.0], 4 * [nan], 4 * [nan]], id="axis=0, n=-2"
            ),
            param(
                1,
                1,
                [[nan, 0.0, 1.0, 2.0], [nan, 4.0, 5.0, 6.0], [nan, 8.0, 9.0, 10.0]],
                id="axis=1, n=1",
            ),
            param(
                1,
                2,
                [[nan, nan, 0.0, 1.0], [nan, nan, 4.0, 5.0], [nan, nan, 8.0, 9.0]],
                id="axis=1, n=1",
            ),
            param(
                1,
                -1,
                [[1.0, 2.0, 3.0, nan], [5.0, 6.0, 7.0, nan], [9.0, 10.0, 11.0, nan]],
                id="axis=1, n=-1",
            ),
            param(
                1,
                -2,
                [[2.0, 3.0, nan, nan], [6.0, 7.0, nan, nan], [10.0, 11.0, nan, nan]],
                id="axis=1, n=-2",
            ),
        ],
    )
    def test_2d(
        self, *, axis: int, n: int, expected_v: Sequence[Sequence[float]]
    ) -> None:
        arr = arange(12, dtype=float).reshape((3, 4))
        result = shift(arr, axis=axis, n=n)
        expected = array(expected_v, dtype=float)
        assert_equal(result, expected)

    def test_error(self) -> None:
        arr = array([], dtype=float)
        with raises(ShiftError, match="Shift must be non-zero"):
            _ = shift(arr, n=0)


class TestShiftBool:
    @mark.parametrize(
        ("n", "expected_v"),
        [
            param(1, [None, True, False], id="n=1"),
            param(2, [None, None, True], id="n=2"),
            param(-1, [False, True, None], id="n=-1"),
            param(-2, [True, None, None], id="n=-2"),
        ],
    )
    @mark.parametrize("fill_value", [param(True), param(False)])
    def test_main(
        self, *, n: int, expected_v: Sequence[bool | None], fill_value: bool
    ) -> None:
        arr = array([True, False, True], dtype=bool)
        result = shift_bool(arr, n=n, fill_value=fill_value)
        expected = array(
            [fill_value if e is None else e for e in expected_v], dtype=bool
        )
        assert_equal(result, expected)
