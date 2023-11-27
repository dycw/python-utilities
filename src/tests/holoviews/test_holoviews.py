from __future__ import annotations

from pathlib import Path

from holoviews import Curve

from utilities.holoviews import apply_cols, relabel_plot, save_plot
from utilities.pathvalidate import valid_path
from utilities.pytest import skipif_not_linux


class TestApplyCols:
    def test_main(self) -> None:
        layout = Curve([]) + Curve([])
        _ = apply_cols(layout, 1)


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
        save_plot(curve, valid_path(tmp_path, "plot.png"))
