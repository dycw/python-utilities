from __future__ import annotations

from typing import TYPE_CHECKING

from pytest import approx, mark, param

from utilities.datetime import MILLISECOND, SECOND
from utilities.psutil import MemoryMonitorService

if TYPE_CHECKING:
    from pathlib import Path


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
