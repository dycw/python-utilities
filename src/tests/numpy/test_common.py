from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from numpy import arange, array, nan
from numpy.testing import assert_equal
from pytest import mark, param, raises

from utilities._numpy.common import ShiftError, shift


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
