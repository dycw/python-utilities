from __future__ import annotations

from typing import TYPE_CHECKING

from utilities.datetime import MILLISECOND, SECOND
from utilities.psutil import MemoryMonitorService

if TYPE_CHECKING:
    from pathlib import Path


class TestMemoryMonitorService:
    async def test_main(self, *, tmp_path: Path) -> None:
        path = tmp_path.joinpath("memory.txt")
        service = MemoryMonitorService(
            freq=10 * MILLISECOND, backoff=100 * MILLISECOND, timeout=SECOND, path=path
        )
        async with service.with_auto_start:
            pass
