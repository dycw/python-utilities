from pathlib import Path

from beartype import beartype
from holoviews import Curve
from pytest import mark

from utilities.holoviews import apply_opts, relabel_plot, save_plot
from utilities.platform import SYSTEM, System


class TestApplyOpts:
    @beartype
    def test_main(self) -> None:
        curve = Curve([])
        _ = apply_opts(curve)


class TestRelabelPlot:
    @beartype
    def test_main(self) -> None:
        curve = Curve([])
        assert not curve.label
        curve = relabel_plot(curve, "label")
        assert curve.label == "label"


class TestSavePlot:
    @mark.skipif(SYSTEM is not System.linux, reason="Linux only")
    @beartype
    def test_main(self, tmp_path: Path) -> None:
        curve = Curve([])
        save_plot(curve, tmp_path.joinpath("plot.png"))
