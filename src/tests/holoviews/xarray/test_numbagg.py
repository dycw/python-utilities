from __future__ import annotations

from hypothesis import given
from hypothesis.strategies import integers

from utilities.holoviews import plot_curve
from utilities.hypothesis import (
    assume_does_not_raise,
    float_data_arrays,
    int_indexes,
    text_ascii,
)
from utilities.xarray import DataArrayF1


class TestPlotCurve:
    @given(
        array=float_data_arrays(dim=int_indexes(), name=text_ascii(min_size=1)),
        smooth=integers(1, 10),
    )
    def test_smooth(self, *, array: DataArrayF1, smooth: int) -> None:
        with assume_does_not_raise(RuntimeWarning):
            curve = plot_curve(array, smooth=smooth)
        assert curve.label == f"{array.name} (MA{smooth})"
