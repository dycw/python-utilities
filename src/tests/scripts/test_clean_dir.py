from __future__ import annotations

import datetime as dt
from pathlib import Path

from click.testing import CliRunner
from freezegun import freeze_time
from hypothesis import given
from hypothesis.strategies import integers

from utilities.datetime import TODAY_UTC
from utilities.hypothesis import temp_paths
from utilities.pytest import skipif_windows
from utilities.scripts.clean_dir import main
from utilities.scripts.clean_dir.classes import Config


class TestCleanDir:
    timedelta = dt.timedelta(days=Config().days + 1)

    @skipif_windows
    def test_file(self, *, tmp_path: Path) -> None:
        Path(tmp_path, "file").touch()
        runner = CliRunner()
        args = ["--path", str(tmp_path)]
        with freeze_time(TODAY_UTC + self.timedelta):
            result = runner.invoke(main, args)
        assert result.exit_code == 0

    @skipif_windows
    def test_dir_to_remove(self, *, tmp_path: Path) -> None:
        Path(tmp_path, "dir").mkdir()
        runner = CliRunner()
        args = ["--path", str(tmp_path)]
        result = runner.invoke(main, args)
        assert result.exit_code == 0

    @skipif_windows
    def test_dir_to_retain(self, *, tmp_path: Path) -> None:
        dir_ = Path(tmp_path, "dir")
        dir_.mkdir()
        Path(dir_, "file").touch()
        runner = CliRunner()
        args = ["--path", str(tmp_path)]
        result = runner.invoke(main, args)
        assert result.exit_code == 0

    @skipif_windows
    def test_symlink(self, *, tmp_path: Path) -> None:
        file = Path(tmp_path, "file")
        file.touch()
        Path(tmp_path, "second").symlink_to(file)
        runner = CliRunner()
        args = ["--path", str(tmp_path)]
        with freeze_time(TODAY_UTC + self.timedelta):
            result = runner.invoke(main, args)
        assert result.exit_code == 0

    @skipif_windows
    @given(root=temp_paths(), chunk_size=integers(1, 10))
    def test_chunk_size(self, *, root: Path, chunk_size: int) -> None:
        Path(root, "file").touch()
        runner = CliRunner()
        args = ["--path", str(root), "--chunk-size", str(chunk_size)]
        with freeze_time(TODAY_UTC + self.timedelta):
            result = runner.invoke(main, args)
        assert result.exit_code == 0

    @skipif_windows
    def test_dry_run(self, *, tmp_path: Path) -> None:
        Path(tmp_path, "file").touch()
        runner = CliRunner()
        args = ["--path", str(tmp_path), "--dry-run"]
        with freeze_time(TODAY_UTC + self.timedelta):
            result = runner.invoke(main, args)
        assert result.exit_code == 0
