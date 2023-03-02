from pathlib import Path

from holoviews import Curve
from pytest import mark

from utilities.holoviews import save_plot


class TestSavePlot:
    @mark.xfail(reason="firefox/geckodrover")
    def test_main(self, tmp_path: Path) -> None:
        curve = Curve([])
        save_plot(curve, tmp_path.joinpath("plot.png"))
