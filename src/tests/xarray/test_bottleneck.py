from __future__ import annotations

from xarray import DataArray


class TestBottleNeckInstalled:
    def test_main(self) -> None:
        array = DataArray([], {"dim": []}, ["dim"])
        _ = array.ffill(dim="dim")
