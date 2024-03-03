from collections.abc import Sequence

import pytest
from hypothesis import given
from hypothesis.strategies import floats, integers
from numpy import array, isfinite, isnan, nan
from numpy.testing import assert_allclose, assert_equal

from utilities.hypothesis import float_arrays
from utilities.numpy import NDArrayF1, is_between_or_nan
from utilities.scipy import ppf


class TestPPF:
    @pytest.mark.parametrize(
        ("values", "expected"),
        [
            pytest.param([], []),
            pytest.param([0.0], [0.0]),
            pytest.param([0.0, 0.1], [-1.0, 1.0]),
            pytest.param([0.0, 0.1, 0.2], [-1.0, 0.0, 1.0]),
            pytest.param([0.0, 0.1, 0.2, 0.3], [-1.0, -0.2891889, 0.2891889, 1.0]),
            pytest.param([nan], [nan]),
            pytest.param([0.0, nan, 0.1], [-1.0, nan, 1.0]),
        ],
    )
    def test_examples(
        self, *, values: Sequence[float], expected: Sequence[float]
    ) -> None:
        result = ppf(array(values, dtype=float), 1.0)
        assert_allclose(result, array(expected, dtype=float))

    @given(
        array=float_arrays(
            shape=integers(0, 10), min_value=-10.0, max_value=10.0, allow_nan=True
        ),
        cutoff=floats(0.0, 10.0),
    )
    def test_main(self, *, array: NDArrayF1, cutoff: float) -> None:
        result = ppf(array, cutoff)
        assert_equal(isfinite(result), isfinite(array))
        assert_equal(isnan(result), isnan(array))
        assert is_between_or_nan(result, -cutoff, cutoff).all()
