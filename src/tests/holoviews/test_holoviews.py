from pathlib import Path

from holoviews import Curve

from utilities.holoviews import apply_opts, relabel_plot, save_plot


class TestApplyOpts:
    def test_main(self) -> None:
        curve = Curve([])
        _ = apply_opts(curve)


class TestRelabelPlot:
    def test_main(self) -> None:
        curve = Curve([])
        assert not curve.label
        curve = relabel_plot(curve, "label")
        assert curve.label == "label"


class TestSavePlot:
    def test_main(self, tmp_path: Path) -> None:
        curve = Curve([])
        save_plot(curve, tmp_path.joinpath("plot.png"))
