from csv import reader
from pathlib import Path

from click.testing import CliRunner

from utilities.monitor_memory import _get_memory_usage
from utilities.monitor_memory import _monitor_memory
from utilities.monitor_memory import main


class TestMonitorMemory:
    def test_cli(self, tmp_path: Path) -> None:
        path = tmp_path.joinpath("memory.csv")
        runner = CliRunner()
        args = ["--path", path.as_posix(), "--freq", "1", "--duration", "1"]
        result = runner.invoke(main, args)
        assert result.exit_code == 0

    def test_monitor_memory(self, tmp_path: Path) -> None:
        path = tmp_path.joinpath("memory.csv")
        _ = _monitor_memory(path=path, freq=1, duration=1)
        assert path.exists()
        with path.open(mode="r") as fh:
            read = reader(fh)
            assert len(list(read)) == 1

    def test_get_memory_usage(self) -> None:
        _ = _get_memory_usage()
