from __future__ import annotations

import gzip
from contextlib import suppress
from itertools import pairwise
from typing import TYPE_CHECKING

from pytest import mark, param, raises

from utilities.atomicwrites import (
    _MoveDirectoryExistsError,
    _MoveFileExistsError,
    _MoveSourceNotFoundError,
    _WriterDirectoryExistsError,
    _WriterFileExistsError,
    _WriterTemporaryPathEmptyError,
    move,
    move_many,
    writer,
)

if TYPE_CHECKING:
    from pathlib import Path


class TestMove:
    @mark.parametrize("overwrite", [param(True), param(False)])
    def test_file_destination_does_not_exist(
        self, *, tmp_path: Path, overwrite: bool
    ) -> None:
        source = tmp_path.joinpath("source")
        _ = source.write_text("text")
        destination = tmp_path.joinpath("destination")
        move(source, destination, overwrite=overwrite)
        assert destination.is_file()
        assert destination.read_text() == "text"

    def test_file_destination_file_exists(self, *, tmp_path: Path) -> None:
        source = tmp_path.joinpath("source")
        _ = source.write_text("source")
        destination = tmp_path.joinpath("destination")
        _ = destination.write_text("destination")
        move(source, destination, overwrite=True)
        assert destination.is_file()
        assert destination.read_text() == "source"

    def test_file_destination_directory_exists(self, *, tmp_path: Path) -> None:
        source = tmp_path.joinpath("source")
        _ = source.write_text("source")
        destination = tmp_path.joinpath("destination")
        destination.mkdir()
        move(source, destination, overwrite=True)
        assert destination.is_file()
        assert destination.read_text() == "source"

    @mark.parametrize("overwrite", [param(True), param(False)])
    def test_directory_destination_does_not_exist(
        self, *, tmp_path: Path, overwrite: bool
    ) -> None:
        source = tmp_path.joinpath("source")
        source.mkdir()
        source.joinpath("file").touch()
        destination = tmp_path.joinpath("destination")
        move(source, destination, overwrite=overwrite)
        assert destination.is_dir()
        assert len(list(destination.iterdir())) == 1

    def test_directory_destination_file_exists(self, *, tmp_path: Path) -> None:
        source = tmp_path.joinpath("source")
        source.mkdir()
        source.joinpath("file").touch()
        destination = tmp_path.joinpath("destination")
        destination.touch()
        move(source, destination, overwrite=True)
        assert destination.is_dir()
        assert len(list(destination.iterdir())) == 1

    def test_directory_destination_directory_exists(self, *, tmp_path: Path) -> None:
        source = tmp_path.joinpath("source")
        source.mkdir()
        source.joinpath("file").touch()
        destination = tmp_path.joinpath("destination")
        destination.mkdir()
        for i in range(2):
            destination.joinpath(f"file{i}").touch()
        move(source, destination, overwrite=True)
        assert destination.is_dir()
        assert len(list(destination.iterdir())) == 1

    @mark.parametrize("overwrite", [param(True), param(False)])
    def test_error_source_not_found(self, *, tmp_path: Path, overwrite: bool) -> None:
        with raises(_MoveSourceNotFoundError, match="Source '.*' does not exist"):
            move(
                tmp_path.joinpath("source"),
                tmp_path.joinpath("destination"),
                overwrite=overwrite,
            )

    def test_error_file_exists(self, *, tmp_path: Path) -> None:
        source = tmp_path.joinpath("source")
        source.touch()
        destination = tmp_path.joinpath("destination")
        destination.touch()
        with raises(
            _MoveFileExistsError,
            match="Cannot move file '.*' as destination '.*' already exists",
        ):
            move(source, destination)

    def test_error_directory_exists(self, *, tmp_path: Path) -> None:
        source = tmp_path.joinpath("source")
        source.mkdir()
        destination = tmp_path.joinpath("destination")
        destination.touch()
        with raises(
            _MoveDirectoryExistsError,
            match="Cannot move directory '.*' as destination '.*' already exists",
        ):
            move(source, destination)


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
    def test_main(self, *, tmp_path: Path) -> None:
        path = tmp_path.joinpath("file.txt")
        with writer(path) as temp:
            _ = temp.write_text("contents")
        assert path.is_file()
        assert path.read_text() == "contents"

    def test_gzip(self, *, tmp_path: Path) -> None:
        path = tmp_path.joinpath("file.txt")
        with writer(path, compress=True) as temp:
            _ = temp.write_bytes(b"contents")
        assert path.is_file()
        with gzip.open(path) as gz:
            assert gz.read() == b"contents"

    def test_error_temporary_path_empty(self, *, tmp_path: Path) -> None:
        with (
            raises(
                _WriterTemporaryPathEmptyError, match="Temporary path '.*' is empty"
            ),
            writer(tmp_path),
        ):
            pass

    def test_error_file_exists(self, *, tmp_path: Path) -> None:
        path = tmp_path.joinpath("file.txt")
        path.touch()
        with (
            raises(
                _WriterFileExistsError,
                match="Cannot write to '.*' as file already exists",
            ),
            writer(path) as temp,
        ):
            _ = temp.write_text("new contents")

    def test_error_directory_exists(self, *, tmp_path: Path) -> None:
        path = tmp_path.joinpath("dir")
        path.mkdir()
        with (
            raises(
                _WriterDirectoryExistsError,
                match="Cannot write to '.*' as directory already exists",
            ),
            writer(path) as temp,
        ):
            temp.mkdir()

    @mark.parametrize(
        ("error", "expected"),
        [param(KeyboardInterrupt, False), param(ValueError, True)],
    )
    def test_error_during_write(
        self, *, tmp_path: Path, error: type[Exception], expected: bool
    ) -> None:
        path = tmp_path.joinpath("file.txt")

        def raise_error() -> None:
            raise error

        with writer(path) as temp, suppress(Exception):
            _ = temp.write_text("contents")
            raise_error()
        is_non_empty = len(list(tmp_path.iterdir())) >= 1
        assert is_non_empty is expected
