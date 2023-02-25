import datetime as dt
from pathlib import Path

from click.testing import CliRunner
from freezegun import freeze_time
from hypothesis import given
from hypothesis.strategies import integers

from utilities.clean_dir import main
from utilities.clean_dir.classes import Config
from utilities.datetime import UTC
from utilities.hypothesis import temp_paths


class TestCleanDir:
    timedelta = dt.timedelta(days=Config().days + 1)

    def test_file(self, tmp_path: Path) -> None:
        tmp_path.joinpath("file").touch()
        runner = CliRunner()
        args = ["--path", tmp_path.as_posix()]
        with freeze_time(dt.datetime.now(tz=UTC).date() + self.timedelta):
            result = runner.invoke(main, args)
        assert result.exit_code == 0

    def test_dir_to_remove(self, tmp_path: Path) -> None:
        tmp_path.joinpath("dir").mkdir()
        runner = CliRunner()
        args = ["--path", tmp_path.as_posix()]
        result = runner.invoke(main, args)
        assert result.exit_code == 0

    def test_dir_to_retain(self, tmp_path: Path) -> None:
        dir_ = tmp_path.joinpath("dir")
        dir_.mkdir()
        dir_.joinpath("file").touch()
        runner = CliRunner()
        args = ["--path", tmp_path.as_posix()]
        result = runner.invoke(main, args)
        assert result.exit_code == 0

    def test_symlink(self, tmp_path: Path) -> None:
        file = tmp_path.joinpath("file")
        file.touch()
        tmp_path.joinpath("second").symlink_to(file)
        runner = CliRunner()
        args = ["--path", tmp_path.as_posix()]
        with freeze_time(dt.datetime.now(tz=UTC).date() + self.timedelta):
            result = runner.invoke(main, args)
        assert result.exit_code == 0

    @given(temp_path=temp_paths(), chunk_size=integers(1, 10))
    def test_chunk_size(self, temp_path: Path, chunk_size: int) -> None:
        temp_path.joinpath("file").touch()
        runner = CliRunner()
        args = ["--path", temp_path.as_posix(), "--chunk-size", str(chunk_size)]
        with freeze_time(dt.datetime.now(tz=UTC).date() + self.timedelta):
            result = runner.invoke(main, args)
        assert result.exit_code == 0

    def test_dry_run(self, tmp_path: Path) -> None:
        tmp_path.joinpath("file").touch()
        runner = CliRunner()
        args = ["--path", tmp_path.as_posix(), "--dry-run"]
        with freeze_time(dt.datetime.now(tz=UTC).date() + self.timedelta):
            result = runner.invoke(main, args)
        assert result.exit_code == 0
