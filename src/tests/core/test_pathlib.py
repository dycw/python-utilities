from __future__ import annotations

from os import mkfifo
from pathlib import Path

from pytest import raises

from utilities.core import (
    _FileOrDirMissingError,
    _FileOrDirTypeError,
    file_or_dir,
    yield_temp_cwd,
)


class TestFileOrDir:
    def test_file(self, *, tmp_path: Path) -> None:
        path = tmp_path / "file.txt"
        path.touch()
        result = file_or_dir(path)
        assert result == "file"

    def test_dir(self, *, tmp_path: Path) -> None:
        path = tmp_path / "dir"
        path.mkdir()
        result = file_or_dir(path)
        assert result == "dir"

    def test_empty(self, *, tmp_path: Path) -> None:
        path = tmp_path / "non-existent"
        result = file_or_dir(path)
        assert result is None

    def test_error_missing(self, *, tmp_path: Path) -> None:
        path = tmp_path / "non-existent"
        with raises(_FileOrDirMissingError, match=r"Path does not exist: '.*'"):
            _ = file_or_dir(path, exists=True)

    def test_error_type(self, *, tmp_path: Path) -> None:
        path = tmp_path / "fifo"
        mkfifo(path)
        with raises(
            _FileOrDirTypeError, match=r"Path is neither a file nor a directory: '.*'"
        ):
            _ = file_or_dir(path)


class TestYieldTempCwd:
    def test_main(self, *, tmp_path: Path) -> None:
        assert Path.cwd() != tmp_path
        with yield_temp_cwd(tmp_path):
            assert Path.cwd() == tmp_path
        assert Path.cwd() != tmp_path
