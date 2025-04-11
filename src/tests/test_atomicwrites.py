from __future__ import annotations

import itertools
from contextlib import suppress
from typing import TYPE_CHECKING

from hypothesis import given
from hypothesis.strategies import booleans, sampled_from
from pytest import raises

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
from utilities.hypothesis import settings_with_reduced_examples, temp_paths

if TYPE_CHECKING:
    from pathlib import Path

    from utilities.types import OpenMode


class TestMove:
    @given(root=temp_paths(), overwrite=booleans())
    def test_file_destination_does_not_exist(
        self, *, root: Path, overwrite: bool
    ) -> None:
        source = root.joinpath("source")
        with source.open(mode="w") as fh:
            _ = fh.write("text")
        destination = root.joinpath("destination")
        move(source, destination, overwrite=overwrite)
        assert destination.is_file()
        with destination.open() as fh:
            assert fh.read() == "text"

    @given(root=temp_paths())
    def test_file_destination_file_exists(self, *, root: Path) -> None:
        source = root.joinpath("source")
        with source.open(mode="w") as fh:
            _ = fh.write("source")
        destination = root.joinpath("destination")
        with destination.open(mode="w") as fh:
            _ = fh.write("destination")
        move(source, destination, overwrite=True)
        assert destination.is_file()
        with destination.open() as fh:
            assert fh.read() == "source"

    @given(root=temp_paths())
    def test_file_destination_directory_exists(self, *, root: Path) -> None:
        source = root.joinpath("source")
        with source.open(mode="w") as fh:
            _ = fh.write("source")
        destination = root.joinpath("destination")
        destination.mkdir()
        move(source, destination, overwrite=True)
        assert destination.is_file()
        with destination.open() as fh:
            assert fh.read() == "source"

    @given(root=temp_paths(), overwrite=booleans())
    def test_directory_destination_does_not_exist(
        self, *, root: Path, overwrite: bool
    ) -> None:
        source = root.joinpath("source")
        source.mkdir()
        source.joinpath("file").touch()
        destination = root.joinpath("destination")
        move(source, destination, overwrite=overwrite)
        assert destination.is_dir()
        assert len(list(destination.iterdir())) == 1

    @given(root=temp_paths())
    def test_directory_destination_file_exists(self, *, root: Path) -> None:
        source = root.joinpath("source")
        source.mkdir()
        source.joinpath("file").touch()
        destination = root.joinpath("destination")
        destination.touch()
        move(source, destination, overwrite=True)
        assert destination.is_dir()
        assert len(list(destination.iterdir())) == 1

    @given(root=temp_paths())
    def test_directory_destination_directory_exists(self, *, root: Path) -> None:
        source = root.joinpath("source")
        source.mkdir()
        source.joinpath("file").touch()
        destination = root.joinpath("destination")
        destination.mkdir()
        for i in range(2):
            destination.joinpath(f"file{i}").touch()
        move(source, destination, overwrite=True)
        assert destination.is_dir()
        assert len(list(destination.iterdir())) == 1

    @given(root=temp_paths(), overwrite=booleans())
    def test_error_source_not_found(self, *, root: Path, overwrite: bool) -> None:
        with raises(_MoveSourceNotFoundError, match="Source '.*' does not exist"):
            move(
                root.joinpath("source"),
                root.joinpath("destination"),
                overwrite=overwrite,
            )

    @given(root=temp_paths())
    def test_error_file_exists(self, *, root: Path) -> None:
        source = root.joinpath("source")
        source.touch()
        destination = root.joinpath("destination")
        destination.touch()
        with raises(
            _MoveFileExistsError,
            match="Cannot move file '.*' as destination '.*' already exists",
        ):
            move(source, destination)

    @given(root=temp_paths())
    def test_error_directory_exists(self, *, root: Path) -> None:
        source = root.joinpath("source")
        source.mkdir()
        destination = root.joinpath("destination")
        destination.touch()
        with raises(
            _MoveDirectoryExistsError,
            match="Cannot move directory '.*' as destination '.*' already exists",
        ):
            move(source, destination)


class TestMoveMany:
    @given(root=temp_paths())
    @settings_with_reduced_examples()
    def test_many(self, *, root: Path) -> None:
        n = 5
        files = [root.joinpath(f"file{i}") for i in range(n + 1)]
        for i in range(n):
            with files[i].open(mode="w") as fh:
                _ = fh.write(str(i))
        move_many(*itertools.pairwise(files), overwrite=True)
        for i in range(1, n + 1):
            with files[i].open() as fh:
                assert fh.read() == str(i - 1)


class TestWriter:
    @given(
        root=temp_paths(),
        case=sampled_from([("w", "r", "contents"), ("wb", "rb", b"contents")]),
    )
    def test_file_writing(
        self, *, root: Path, case: tuple[OpenMode, OpenMode, str | bytes]
    ) -> None:
        write_mode, read_mode, contents = case
        path = root.joinpath("file.txt")
        with writer(path) as temp, temp.open(mode=write_mode) as fh1:
            _ = fh1.write(contents)
        with path.open(mode=read_mode) as fh2:
            assert fh2.read() == contents

    @given(root=temp_paths())
    def test_error_temporary_path_empty(self, *, root: Path) -> None:
        with (
            raises(
                _WriterTemporaryPathEmptyError, match="Temporary path '.*' is empty"
            ),
            writer(root),
        ):
            pass

    @given(root=temp_paths())
    def test_error_file_exists(self, *, root: Path) -> None:
        path = root.joinpath("file.txt")
        path.touch()
        with (
            raises(
                _WriterFileExistsError,
                match="Cannot write to '.*' as file already exists",
            ),
            writer(path) as temp,
            temp.open(mode="w") as fh,
        ):
            _ = fh.write("new contents")

    @given(root=temp_paths())
    def test_error_directory_exists(self, *, root: Path) -> None:
        path = root.joinpath("dir")
        path.mkdir()
        with (
            raises(
                _WriterDirectoryExistsError,
                match="Cannot write to '.*' as directory already exists",
            ),
            writer(path) as temp,
        ):
            temp.mkdir()

    @given(
        root=temp_paths(),
        case=sampled_from([(KeyboardInterrupt, False), (ValueError, True)]),
    )
    def test_error_during_write(
        self, *, root: Path, case: tuple[type[Exception], bool]
    ) -> None:
        error, expected = case
        path = root.joinpath("file.txt")

        def raise_error() -> None:
            raise error

        with writer(path) as temp1, temp1.open(mode="w") as fh, suppress(Exception):
            _ = fh.write("contents")
            raise_error()
        is_non_empty = len(list(root.iterdir())) >= 1
        assert is_non_empty is expected
