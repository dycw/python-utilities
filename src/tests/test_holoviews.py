from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import pytest
from holoviews import Curve
from hypothesis import given
from hypothesis.strategies import floats, integers

from utilities.holoviews import (
    PlotCurveError,
    apply_cols,
    apply_opts,
    plot_curve,
    relabel_plot,
    save_plot,
)
from utilities.hypothesis import (
    bool_data_arrays,
    float_data_arrays,
    int_indexes,
    text_ascii,
)
from utilities.pytest import skipif_not_linux

if TYPE_CHECKING:
    from utilities.xarray import DataArrayB1, DataArrayF1


class TestApplyCols:
    def test_main(self) -> None:
        layout = Curve([]) + Curve([])
        _ = apply_cols(layout, 1)


class TestApplyOpts:
    def test_main(self) -> None:
        curve = Curve([])
        _ = apply_opts(curve)


class TestPlotCurve:
    @given(
        array=float_data_arrays(
            min_value=-1.0,
            max_value=1.0,
            dim=int_indexes(),
            name=text_ascii(min_size=1),
        )
    )
    def test_main(self, *, array: DataArrayF1) -> None:
        curve = plot_curve(array)
        assert curve.kdims == ["dim"]
        assert curve.vdims == [array.name]
        assert curve.label == array.name

    @given(
        array=float_data_arrays(
            min_value=-1.0,
            max_value=1.0,
            dim=int_indexes(),
            name=text_ascii(min_size=1),
        ),
        label=text_ascii(min_size=1),
    )
    def test_label(self, *, array: DataArrayF1, label: str) -> None:
        curve = plot_curve(array, label=label)
        assert curve.label == label

    @given(
        array=float_data_arrays(
            min_value=-1.0,
            max_value=1.0,
            dim=int_indexes(),
            name=text_ascii(min_size=1),
        ),
        aspect=floats(1.0, 10.0),
    )
    def test_aspect(self, *, array: DataArrayF1, aspect: float) -> None:
        _ = plot_curve(array, aspect=aspect)

    @given(
        array=float_data_arrays(
            min_value=-1.0,
            max_value=1.0,
            dim=int_indexes(),
            name=text_ascii(min_size=1),
        ),
        smooth=integers(1, 10),
    )
    def test_smooth(self, *, array: DataArrayF1, smooth: int) -> None:
        _ = plot_curve(array, smooth=smooth)

    @given(array=float_data_arrays(dim=int_indexes()))
    def test_array_name_not_a_string(self, *, array: DataArrayF1) -> None:
        with pytest.raises(
            PlotCurveError, match="Array name .* must be a string; got .* instead"
        ):
            _ = plot_curve(array)

    @given(array=float_data_arrays(dim=int_indexes(), name=text_ascii(max_size=0)))
    def test_array_name_is_empty_string(self, *, array: DataArrayF1) -> None:
        with pytest.raises(PlotCurveError, match="Array name .* must not be empty"):
            _ = plot_curve(array)

    @given(array=bool_data_arrays(dim=int_indexes(), name=text_ascii(min_size=1)))
    def test_boolean(self, *, array: DataArrayB1) -> None:
        _ = plot_curve(array)


class TestRelabelPlot:
    def test_main(self) -> None:
        curve = Curve([])
        assert curve.label == ""
        curve = relabel_plot(curve, "label")
        assert curve.label == "label"


class TestSavePlot:
    @skipif_not_linux
    def test_main(self, *, tmp_path: Path) -> None:
        curve = Curve([])
        save_plot(curve, Path(tmp_path, "plot.png"))
