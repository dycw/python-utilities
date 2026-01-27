from __future__ import annotations

from os import mkfifo
from pathlib import Path

from pytest import raises

from utilities._core_errors import (
    FirstNonDirectoryParentError,
    ReadTextIfExistingFileIsADirectoryError,
    ReadTextIfExistingFileNotADirectoryError,
)
from utilities.core import (
    FileOrDirMissingError,
    FileOrDirTypeError,
    file_or_dir,
    first_non_directory_parent,
    read_text_if_existing_file,
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
        with raises(FileOrDirMissingError, match=r"Path does not exist: '.*'"):
            _ = file_or_dir(path, exists=True)

    def test_error_type(self, *, tmp_path: Path) -> None:
        path = tmp_path / "fifo"
        mkfifo(path)
        with raises(
            FileOrDirTypeError, match=r"Path is neither a file nor a directory: '.*'"
        ):
            _ = file_or_dir(path)


class TestFirstNonDirectoryParent:
    def test_depth1(self, *, temp_file: Path) -> None:
        path = temp_file / "foo"
        assert first_non_directory_parent(path) == temp_file

    def test_depth2(self, *, temp_file: Path) -> None:
        path = temp_file / "foo/bar"
        assert first_non_directory_parent(path) == temp_file

    def test_error(self, *, tmp_path: Path) -> None:
        with raises(
            FirstNonDirectoryParentError,
            match=r"Path has no non-directory parents: '.*'",
        ):
            _ = first_non_directory_parent(tmp_path)


class TestReadTextIfExistingFile:
    def test_existing_file(self, *, temp_file: Path) -> None:
        _ = temp_file.write_text("text")
        assert read_text_if_existing_file(temp_file) == "text"

    def test_not_existing(self, *, temp_path_not_exist: Path) -> None:
        assert read_text_if_existing_file(temp_path_not_exist) == str(
            temp_path_not_exist
        )

    def test_error_is_a_directory(self, *, tmp_path: Path) -> None:
        with raises(
            ReadTextIfExistingFileIsADirectoryError,
            match=r"Cannot read from '.*' since it is a directory",
        ):
            _ = read_text_if_existing_file(tmp_path)

    def test_error_not_a_directory(self, *, temp_path_parent_file: Path) -> None:
        with raises(
            ReadTextIfExistingFileNotADirectoryError,
            match=r"Cannot read from '.*' since its parent '.*' is not a directory",
        ):
            _ = read_text_if_existing_file(temp_path_parent_file)


class TestYieldTempCwd:
    def test_main(self, *, tmp_path: Path) -> None:
        assert Path.cwd() != tmp_path
        with yield_temp_cwd(tmp_path):
            assert Path.cwd() == tmp_path
        assert Path.cwd() != tmp_path
