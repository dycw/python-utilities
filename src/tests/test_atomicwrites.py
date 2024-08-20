from __future__ import annotations

from contextlib import suppress
from pathlib import Path
from re import escape

from pytest import mark, param, raises

from utilities.atomicwrites import DirectoryExistsError, WriterError, writer
from utilities.platform import IS_WINDOWS


class TestWriter:
    @mark.parametrize(
        ("is_binary", "contents"),
        [param(False, "contents", id="text"), param(True, b"contents", id="binary")],
    )
    def test_file_writing(
        self, *, tmp_path: Path, is_binary: bool, contents: str | bytes
    ) -> None:
        path = Path(tmp_path, "file.txt")
        with writer(path) as temp, temp.open(mode="wb" if is_binary else "w") as fh1:
            _ = fh1.write(contents)
        with path.open(mode="rb" if is_binary else "r") as fh2:
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

    def test_dir_exists_error(self, *, tmp_path: Path) -> None:
        path = Path(tmp_path, "dir")
        with writer(path) as temp1:
            temp1.mkdir()
        with raises(DirectoryExistsError), writer(path) as temp2:
            temp2.mkdir()

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

    @mark.parametrize("error", [param(KeyboardInterrupt), param(ValueError)])
    def test_error_during_write(
        self, *, tmp_path: Path, error: type[Exception]
    ) -> None:
        path = Path(tmp_path, "file.txt")

        def raise_error() -> None:
            raise error

        with writer(path) as temp1, temp1.open(mode="w") as fh, suppress(Exception):
            _ = fh.write("contents")
            raise_error()
        expected = int(not issubclass(error, KeyboardInterrupt))
        assert len(list(tmp_path.iterdir())) == expected

    def test_writer(self, *, tmp_path: Path) -> None:
        path = Path(tmp_path, "file.txt")
        with raises(WriterError), writer(path):
            pass
