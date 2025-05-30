from __future__ import annotations

from math import isfinite, isnan
from typing import TYPE_CHECKING

from pytest import approx, mark, param

from utilities.datetime import MILLISECOND, SECOND
from utilities.psutil import MemoryMonitorService, MemoryUsage

if TYPE_CHECKING:
    from pathlib import Path


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


class TestMemoryMonitorService:
    @mark.parametrize("console", [param(True), param(False)])
    async def test_main(self, *, console: bool, tmp_path: Path) -> None:
        path = tmp_path.joinpath("memory.txt")
        service = MemoryMonitorService(
            freq=100 * MILLISECOND,
            backoff=100 * MILLISECOND,
            timeout=SECOND,
            path=path,
            console=str(tmp_path) if console else None,
        )
        async with service.with_auto_start:
            ...
        assert path.exists()
        with path.open() as fh:
            lines = fh.readlines()
        assert len(lines) == approx(10, rel=0.5)
