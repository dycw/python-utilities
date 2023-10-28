from __future__ import annotations

import datetime as dt
from pathlib import Path

from click.testing import CliRunner
from freezegun import freeze_time
from hypothesis import given
from hypothesis.strategies import integers
from pytest import mark

from utilities.clean_dir import main
from utilities.clean_dir.classes import Config
from utilities.datetime import TODAY
from utilities.hypothesis import temp_paths
from utilities.platform import IS_WINDOWS


class TestCleanDir:
    timedelta = dt.timedelta(days=Config().days + 1)

    @mark.skipif(condition=IS_WINDOWS, reason="non-Windows only")
    def test_file(self, *, tmp_path: Path) -> None:
        tmp_path.joinpath("file").touch()
        runner = CliRunner()
        args = ["--path", str(tmp_path)]
        with freeze_time(TODAY + self.timedelta):
            result = runner.invoke(main, args)
        assert result.exit_code == 0

    @mark.skipif(condition=IS_WINDOWS, reason="non-Windows only")
    def test_dir_to_remove(self, *, tmp_path: Path) -> None:
        tmp_path.joinpath("dir").mkdir()
        runner = CliRunner()
        args = ["--path", str(tmp_path)]
        result = runner.invoke(main, args)
        assert result.exit_code == 0

    @mark.skipif(condition=IS_WINDOWS, reason="non-Windows only")
    def test_dir_to_retain(self, *, tmp_path: Path) -> None:
        dir_ = tmp_path.joinpath("dir")
        dir_.mkdir()
        dir_.joinpath("file").touch()
        runner = CliRunner()
        args = ["--path", str(tmp_path)]
        result = runner.invoke(main, args)
        assert result.exit_code == 0

    @mark.skipif(condition=IS_WINDOWS, reason="non-Windows only")
    def test_symlink(self, *, tmp_path: Path) -> None:
        file = tmp_path.joinpath("file")
        file.touch()
        tmp_path.joinpath("second").symlink_to(file)
        runner = CliRunner()
        args = ["--path", str(tmp_path)]
        with freeze_time(TODAY + self.timedelta):
            result = runner.invoke(main, args)
        assert result.exit_code == 0

    @mark.skipif(condition=IS_WINDOWS, reason="non-Windows only")
    @given(root=temp_paths(), chunk_size=integers(1, 10))
    def test_chunk_size(self, *, root: Path, chunk_size: int) -> None:
        root.joinpath("file").touch()
        runner = CliRunner()
        args = ["--path", str(root), "--chunk-size", str(chunk_size)]
        with freeze_time(TODAY + self.timedelta):
            result = runner.invoke(main, args)
        assert result.exit_code == 0

    @mark.skipif(condition=IS_WINDOWS, reason="non-Windows only")
    def test_dry_run(self, *, tmp_path: Path) -> None:
        tmp_path.joinpath("file").touch()
        runner = CliRunner()
        args = ["--path", str(tmp_path), "--dry-run"]
        with freeze_time(TODAY + self.timedelta):
            result = runner.invoke(main, args)
        assert result.exit_code == 0
