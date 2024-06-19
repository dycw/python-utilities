from __future__ import annotations

from csv import reader
from typing import TYPE_CHECKING

from click.testing import CliRunner

from utilities.pathlib import ensure_path
from utilities.scripts.monitor_memory import _get_memory_usage, _monitor_memory, main

if TYPE_CHECKING:
    from pathlib import Path


class TestMonitorMemory:
    def test_cli(self, *, tmp_path: Path) -> None:
        path = ensure_path(tmp_path, "memory.csv")
        runner = CliRunner()
        args = ["--path", str(path), "--freq", "1", "--duration", "1"]
        result = runner.invoke(main, args)
        assert result.exit_code == 0

    def test_monitor_memory(self, *, tmp_path: Path) -> None:
        path = ensure_path(tmp_path, "memory.csv")
        _ = _monitor_memory(path=path, freq=1, duration=1)
        assert path.exists()
        with path.open(mode="r") as fh:
            read = reader(fh)
            assert len(list(read)) <= 2

    def test_get_memory_usage(self) -> None:
        _ = _get_memory_usage()
