from __future__ import annotations

from hypothesis import given
from hypothesis.strategies import DataObject, data, floats, integers

from utilities.hypothesis import assume_does_not_raise, float_arrays
from utilities.numpy import NDArrayF, ewma, exp_moving_sum


class TestEwma:
    @given(data=data(), array=float_arrays(), halflife=floats(0.1, 10.0))
    def test_main(self, data: DataObject, array: NDArrayF, halflife: float) -> None:
        axis = data.draw(integers(0, array.ndim - 1)) if array.ndim >= 1 else -1
        with assume_does_not_raise(RuntimeWarning):
            _ = ewma(array, halflife, axis=axis)


class TestExpMovingSum:
    @given(data=data(), array=float_arrays(), halflife=floats(0.1, 10.0))
    def test_main(self, data: DataObject, array: NDArrayF, halflife: float) -> None:
        axis = data.draw(integers(0, array.ndim - 1)) if array.ndim >= 1 else -1
        with assume_does_not_raise(RuntimeWarning):
            _ = exp_moving_sum(array, halflife, axis=axis)
