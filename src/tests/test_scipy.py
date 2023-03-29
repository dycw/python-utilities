from beartype import beartype
from hypothesis import given
from hypothesis.strategies import floats, integers
from numpy import array, isfinite, isnan, nan
from numpy.testing import assert_allclose, assert_equal
from pytest import mark, param

from utilities.hypothesis.numpy import float_arrays
from utilities.numpy import is_between_or_nan
from utilities.numpy.typing import NDArrayF1
from utilities.scipy import ppf


class TestPPF:
    @mark.parametrize(
        ("values", "expected"),
        [
            param([], []),
            param([0.0], [0.0]),
            param([0.0, 0.1], [-1.0, 1.0]),
            param([0.0, 0.1, 0.2], [-1.0, 0.0, 1.0]),
            param([0.0, 0.1, 0.2, 0.3], [-1.0, -0.2891889, 0.2891889, 1.0]),
            param([nan], [nan]),
            param([0.0, nan, 0.1], [-1.0, nan, 1.0]),
        ],
    )
    @beartype
    def test_examples(self, values: list[float], expected: list[float]) -> None:
        result = ppf(array(values, dtype=float), 1.0)
        assert_allclose(result, array(expected, dtype=float))

    @given(
        array=float_arrays(
            shape=integers(0, 10), min_value=-10.0, max_value=10.0, allow_nan=True
        ),
        cutoff=floats(0.0, 10.0),
    )
    @beartype
    def test_main(self, array: NDArrayF1, cutoff: float) -> None:
        result = ppf(array, cutoff)
        assert_equal(isfinite(result), isfinite(array))
        assert_equal(isnan(result), isnan(array))
        assert is_between_or_nan(result, -cutoff, cutoff).all()
