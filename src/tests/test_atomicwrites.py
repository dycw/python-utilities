from __future__ import annotations

import gzip
from contextlib import suppress
from itertools import pairwise
from typing import TYPE_CHECKING

from pytest import mark, param, raises

from utilities.atomicwrites import (
    _CopyDirectoryExistsError,
    _CopyFileExistsError,
    _CopySourceNotFoundError,
    _MoveDirectoryExistsError,
    _MoveFileExistsError,
    _MoveSourceNotFoundError,
    _WriterDirectoryExistsError,
    _WriterFileExistsError,
    _WriterTemporaryPathEmptyError,
    copy,
    move,
    move_many,
    writer,
)

if TYPE_CHECKING:
    from pathlib import Path


class TestCopy:
    @mark.parametrize("overwrite", [param(True), param(False)])
    def test_file_destination_does_not_exist(
        self, *, tmp_path: Path, temp_file: Path, overwrite: bool
    ) -> None:
        _ = temp_file.write_text("text")
        dest = tmp_path.joinpath("dest")
        copy(temp_file, dest, overwrite=overwrite)
        assert temp_file.is_file()
        assert dest.is_file()
        assert dest.read_text() == "text"

    def test_file_destination_file_exists(
        self, *, tmp_path: Path, temp_file: Path
    ) -> None:
        _ = temp_file.write_text("init")
        dest = tmp_path.joinpath("dest")
        _ = dest.write_text("overwrite")
        copy(temp_file, dest, overwrite=True)
        assert temp_file.is_file()
        assert dest.is_file()
        assert dest.read_text() == "init"

    def test_file_destination_directory_exists(
        self, *, tmp_path: Path, temp_file: Path
    ) -> None:
        _ = temp_file.write_text("text")
        dest = tmp_path.joinpath("dest")
        dest.mkdir()
        copy(temp_file, dest, overwrite=True)
        assert temp_file.is_file()
        assert dest.is_file()
        assert dest.read_text() == "text"

    @mark.parametrize("overwrite", [param(True), param(False)])
    def test_directory_destination_does_not_exist(
        self, *, tmp_path: Path, temp_dir_with_file: Path, overwrite: bool
    ) -> None:
        dest = tmp_path.joinpath("dest")
        copy(temp_dir_with_file, dest, overwrite=overwrite)
        assert temp_dir_with_file.is_dir()
        assert dest.is_dir()
        assert len(list(dest.iterdir())) == 1

    def test_directory_destination_file_exists(
        self, *, tmp_path: Path, temp_dir_with_file: Path
    ) -> None:
        dest = tmp_path.joinpath("dest")
        dest.touch()
        copy(temp_dir_with_file, dest, overwrite=True)
        assert temp_dir_with_file.is_dir()
        assert dest.is_dir()
        assert len(list(dest.iterdir())) == 1

    def test_directory_destination_directory_exists(
        self, *, tmp_path: Path, temp_dir_with_file: Path
    ) -> None:
        dest = tmp_path.joinpath("dest")
        dest.mkdir()
        for i in range(2):
            dest.joinpath(f"file{i}").touch()
        copy(temp_dir_with_file, dest, overwrite=True)
        assert temp_dir_with_file.is_dir()
        assert dest.is_dir()
        assert len(list(dest.iterdir())) == 1

    def test_error_source_not_found(
        self, *, tmp_path: Path, temp_path_not_exist: Path
    ) -> None:
        with raises(_CopySourceNotFoundError, match=r"Source '.*' does not exist"):
            copy(temp_path_not_exist, tmp_path)

    def test_error_file_exists(self, *, tmp_path: Path, temp_file: Path) -> None:
        with raises(
            _CopyFileExistsError,
            match=r"Cannot copy file '.*' as destination '.*' already exists",
        ):
            copy(temp_file, temp_file)

    def test_error_directory_exists(self, *, tmp_path: Path) -> None:
        with raises(
            _CopyDirectoryExistsError,
            match=r"Cannot copy directory '.*' as destination '.*' already exists",
        ):
            copy(tmp_path, tmp_path)


class TestMove:
    @mark.parametrize("overwrite", [param(True), param(False)])
    def test_file_destination_does_not_exist(
        self, *, tmp_path: Path, temp_file: Path, overwrite: bool
    ) -> None:
        _ = temp_file.write_text("text")
        dest = tmp_path.joinpath("dest")
        move(temp_file, dest, overwrite=overwrite)
        assert not temp_file.exists()
        assert dest.is_file()
        assert dest.read_text() == "text"

    def test_file_destination_file_exists(
        self, *, tmp_path: Path, temp_file: Path
    ) -> None:
        _ = temp_file.write_text("init")
        dest = tmp_path.joinpath("dest")
        _ = dest.write_text("overwrite")
        move(temp_file, dest, overwrite=True)
        assert not temp_file.exists()
        assert dest.is_file()
        assert dest.read_text() == "init"

    def test_file_destination_directory_exists(
        self, *, tmp_path: Path, temp_file: Path
    ) -> None:
        _ = temp_file.write_text("text")
        dest = tmp_path.joinpath("dest")
        dest.mkdir()
        move(temp_file, dest, overwrite=True)
        assert not temp_file.exists()
        assert dest.is_file()
        assert dest.read_text() == "text"

    @mark.parametrize("overwrite", [param(True), param(False)])
    def test_directory_destination_does_not_exist(
        self, *, tmp_path: Path, temp_dir_with_file: Path, overwrite: bool
    ) -> None:
        dest = tmp_path.joinpath("dest")
        move(temp_dir_with_file, dest, overwrite=overwrite)
        assert not temp_dir_with_file.exists()
        assert dest.is_dir()
        assert len(list(dest.iterdir())) == 1

    def test_directory_destination_file_exists(
        self, *, tmp_path: Path, temp_dir_with_file: Path
    ) -> None:
        dest = tmp_path.joinpath("dest")
        dest.touch()
        move(temp_dir_with_file, dest, overwrite=True)
        assert not temp_dir_with_file.exists()
        assert dest.is_dir()
        assert len(list(dest.iterdir())) == 1

    def test_directory_destination_directory_exists(
        self, *, tmp_path: Path, temp_dir_with_file: Path
    ) -> None:
        dest = tmp_path.joinpath("dest")
        dest.mkdir()
        for i in range(2):
            dest.joinpath(f"file{i}").touch()
        move(temp_dir_with_file, dest, overwrite=True)
        assert not temp_dir_with_file.exists()
        assert dest.is_dir()
        assert len(list(dest.iterdir())) == 1

    def test_error_source_not_found(
        self, *, tmp_path: Path, temp_path_not_exist: Path
    ) -> None:
        with raises(_MoveSourceNotFoundError, match=r"Source '.*' does not exist"):
            move(temp_path_not_exist, tmp_path)

    def test_error_file_exists(self, *, temp_file: Path) -> None:
        with raises(
            _MoveFileExistsError,
            match=r"Cannot move file '.*' as destination '.*' already exists",
        ):
            move(temp_file, temp_file)

    def test_error_directory_exists(self, *, tmp_path: Path) -> None:
        with raises(
            _MoveDirectoryExistsError,
            match=r"Cannot move directory '.*' as destination '.*' already exists",
        ):
            move(tmp_path, tmp_path)


class TestMoveMany:
    def test_many(self, *, tmp_path: Path) -> None:
        n = 5
        files = [tmp_path.joinpath(f"file{i}") for i in range(n + 2)]
        for i, file in enumerate(files[:-1]):
            _ = file.write_text(str(i))
        move_many(*pairwise(files), overwrite=True)
        for i, file in enumerate(files[1:], start=1):
            assert file.read_text() == str(i - 1)


class TestWriter:
    def test_main(self, *, temp_path_not_exist: Path) -> None:
        with writer(temp_path_not_exist) as temp:
            _ = temp.write_text("text")
        assert temp_path_not_exist.is_file()
        assert temp_path_not_exist.read_text() == "text"

    def test_gzip(self, *, temp_path_not_exist: Path) -> None:
        with writer(temp_path_not_exist, compress=True) as temp:
            _ = temp.write_bytes(b"data")
        assert temp_path_not_exist.is_file()
        with gzip.open(temp_path_not_exist) as gz:
            assert gz.read() == b"data"

    def test_error_temporary_path_empty(self, *, tmp_path: Path) -> None:
        with (
            raises(
                _WriterTemporaryPathEmptyError, match=r"Temporary path '.*' is empty"
            ),
            writer(tmp_path),
        ):
            pass

    def test_error_file_exists(self, *, temp_file: Path) -> None:
        with (
            raises(
                _WriterFileExistsError,
                match=r"Cannot write to '.*' as file already exists",
            ),
            writer(temp_file) as temp,
        ):
            _ = temp.write_text("text")

    def test_error_directory_exists(self, *, tmp_path: Path) -> None:
        with (
            raises(
                _WriterDirectoryExistsError,
                match=r"Cannot write to '.*' as directory already exists",
            ),
            writer(tmp_path) as temp,
        ):
            temp.mkdir()

    @mark.parametrize(
        ("error", "expected"),
        [param(KeyboardInterrupt, False), param(ValueError, True)],
    )
    def test_error_during_write(
        self, *, temp_path_not_exist: Path, error: type[Exception], expected: bool
    ) -> None:
        def raise_error() -> None:
            raise error

        with writer(temp_path_not_exist) as temp, suppress(Exception):
            _ = temp.write_text("data")
            raise_error()
        if expected:
            assert temp_path_not_exist.is_file()
        else:
            assert not temp_path_not_exist.exists()
