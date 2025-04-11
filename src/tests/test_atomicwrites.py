from __future__ import annotations

from contextlib import suppress
from pathlib import Path
from re import escape
from typing import TYPE_CHECKING

from hypothesis import given
from hypothesis.strategies import sampled_from
from pytest import raises

from utilities.atomicwrites import _WriterDirectoryExistsError, _WriterTypeError, writer
from utilities.hypothesis import temp_paths
from utilities.platform import IS_WINDOWS

if TYPE_CHECKING:
    from utilities.types import OpenMode


class TestWriter:
    @given(
        root=temp_paths(),
        case=sampled_from([("w", "r", "contents"), ("wb", "rb", b"contents")]),
    )
    def test_file_writing(
        self, *, root: Path, case: tuple[OpenMode, OpenMode, str | bytes]
    ) -> None:
        write_mode, read_mode, contents = case
        path = Path(root, "file.txt")
        with writer(path) as temp, temp.open(mode=write_mode) as fh1:
            _ = fh1.write(contents)
        with path.open(mode=read_mode) as fh2:
            assert fh2.read() == contents

    def test_file_exists_error(self, *, tmp_path: Path) -> None:
        path = Path(tmp_path, "file.txt")
        with writer(path) as temp1, temp1.open(mode="w") as fh1:
            _ = fh1.write("contents")
        match = (
            "Cannot create a file when that file already exists"
            if IS_WINDOWS
            else escape(str(path))
        )
        with (
            raises(FileExistsError, match=match),
            writer(path) as temp2,
            temp2.open(mode="w") as fh2,
        ):
            _ = fh2.write("new contents")

    def test_file_overwrite(self, *, tmp_path: Path) -> None:
        path = Path(tmp_path, "file.txt")
        with writer(path) as temp1, temp1.open(mode="w") as fh1:
            _ = fh1.write("contents")
        with writer(path, overwrite=True) as temp2, temp2.open(mode="w") as fh2:
            _ = fh2.write("new contents")
        with path.open() as fh3:
            assert fh3.read() == "new contents"

    def test_dir_writing(self, *, tmp_path: Path) -> None:
        path = Path(tmp_path, "dir")
        with writer(path) as temp:
            temp.mkdir()
            for i in range(2):
                Path(temp, f"file{i}").touch()
        assert len(list(path.iterdir())) == 2

    def test_dir_overwrite(self, *, tmp_path: Path) -> None:
        path = Path(tmp_path, "dir")
        with writer(path) as temp1:
            temp1.mkdir()
            for i in range(2):
                Path(temp1, f"file{i}").touch()
        with writer(path, overwrite=True) as temp2:
            temp2.mkdir()
            for i in range(3):
                Path(temp2, f"file{i}").touch()
        assert len(list(path.iterdir())) == 3

    @given(
        root=temp_paths(),
        case=sampled_from([(KeyboardInterrupt, False), (ValueError, True)]),
    )
    def test_error_during_write(
        self, *, root: Path, case: tuple[type[Exception], bool]
    ) -> None:
        error, expected = case
        path = Path(root, "file.txt")

        def raise_error() -> None:
            raise error

        with writer(path) as temp1, temp1.open(mode="w") as fh, suppress(Exception):
            _ = fh.write("contents")
            raise_error()
        is_non_empty = len(list(root.iterdir())) >= 1
        assert is_non_empty is expected

    def test_error_directory_exists(self, *, tmp_path: Path) -> None:
        path = Path(tmp_path, "dir")
        with writer(path) as temp1:
            temp1.mkdir()
        with raises(_WriterDirectoryExistsError), writer(path) as temp2:
            temp2.mkdir()

    def test_error_type(self, *, tmp_path: Path) -> None:
        path = Path(tmp_path, "file.txt")
        with raises(_WriterTypeError), writer(path):
            pass
