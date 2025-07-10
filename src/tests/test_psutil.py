from __future__ import annotations

from math import isfinite, isnan

from utilities.psutil import MemoryUsage


class TestMemoryUsage:
    def test_main(self) -> None:
        memory = MemoryUsage.new()
        assert memory.virtual_total >= 0
        assert memory.virtual_total_mb >= 0
        assert memory.virtual_total >= 0
        assert memory.virtual_total_mb >= 0
        assert (
            isfinite(memory.virtual_pct) and (0.0 <= memory.virtual_pct <= 1.0)
        ) or isnan(memory.virtual_pct)
        assert memory.swap_total >= 0
        assert memory.swap_total_mb >= 0
        assert memory.swap_total >= 0
        assert memory.swap_total_mb >= 0
        assert (isfinite(memory.swap_pct) and (0.0 <= memory.swap_pct <= 1.0)) or isnan(
            memory.swap_pct
        )
