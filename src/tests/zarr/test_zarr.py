from __future__ import annotations

from pathlib import Path

from numpy import array, nan
from numpy.testing import assert_equal
from zarr import open_array

from utilities.pathvalidate import valid_path
from utilities.zarr import ffill_non_nan_slices


class TestFFillNonNanSlices:
    def test_main(self, *, tmp_path: Path) -> None:
        arr = array(
            [[0.1, nan, nan, 0.2], 4 * [nan], [0.3, nan, nan, nan]], dtype=float
        )
        z_arr = open_array(valid_path(tmp_path, "array"), shape=arr.shape, dtype=float)
        z_arr[:] = arr
        ffill_non_nan_slices(z_arr)
        expected = array(
            [[0.1, 0.1, 0.1, 0.2], 4 * [nan], [0.3, 0.3, 0.3, nan]], dtype=float
        )
        assert_equal(z_arr[:], expected)
