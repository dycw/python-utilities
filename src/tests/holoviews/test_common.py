from __future__ import annotations

from holoviews import Curve

from utilities._holoviews.common import apply_opts


class TestApplyOpts:
    def test_main(self) -> None:
        curve = Curve([])
        _ = apply_opts(curve)
