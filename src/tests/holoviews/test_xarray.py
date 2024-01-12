from __future__ import annotations

from hypothesis import given
from hypothesis.strategies import floats
from pytest import raises

from utilities.holoviews import PlotCurveError, plot_curve
from utilities.hypothesis import (
    bool_data_arrays,
    float_data_arrays,
    int_indexes,
    text_ascii,
)
from utilities.xarray import DataArrayB1, DataArrayF1


class TestPlotCurve:
    @given(array=float_data_arrays(dim=int_indexes(), name=text_ascii(min_size=1)))
    def test_main(self, *, array: DataArrayF1) -> None:
        curve = plot_curve(array)
        assert curve.kdims == ["dim"]
        assert curve.vdims == [array.name]
        assert curve.label == array.name

    @given(
        array=float_data_arrays(dim=int_indexes(), name=text_ascii(min_size=1)),
        label=text_ascii(min_size=1),
    )
    def test_label(self, *, array: DataArrayF1, label: str) -> None:
        curve = plot_curve(array, label=label)
        assert curve.label == label

    @given(
        array=float_data_arrays(dim=int_indexes(), name=text_ascii(min_size=1)),
        aspect=floats(1.0, 10.0),
    )
    def test_aspect(self, *, array: DataArrayF1, aspect: float) -> None:
        _ = plot_curve(array, aspect=aspect)

    @given(array=float_data_arrays(dim=int_indexes()))
    def test_array_name_not_a_string(self, *, array: DataArrayF1) -> None:
        with raises(
            PlotCurveError, match="Array name .* must be a string; got .* instead"
        ):
            _ = plot_curve(array)

    @given(array=float_data_arrays(dim=int_indexes(), name=text_ascii(max_size=0)))
    def test_array_name_is_empty_string(self, *, array: DataArrayF1) -> None:
        with raises(PlotCurveError, match="Array name .* must not be empty"):
            _ = plot_curve(array)

    @given(array=bool_data_arrays(dim=int_indexes(), name=text_ascii(min_size=1)))
    def test_boolean(self, *, array: DataArrayB1) -> None:
        _ = plot_curve(array)
