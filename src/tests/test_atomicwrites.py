from contextlib import suppress
from pathlib import Path
from typing import Union

from pytest import mark, param, raises

from utilities.atomicwrites import writer
from utilities.errors import DirectoryExistsError


class TestWriter:
    @mark.parametrize(
        ("is_binary", "contents"),
        [param(False, "contents", id="text"), param(True, b"contents", id="binary")],
    )
    def test_file_writing(
        self, tmp_path: Path, is_binary: bool, contents: Union[str, bytes]
    ) -> None:
        path = tmp_path.joinpath("file.txt")
        with writer(path) as temp, temp.open(mode="wb" if is_binary else "w") as fh1:
            _ = fh1.write(contents)
        with path.open(mode="rb" if is_binary else "r") as fh2:
            assert fh2.read() == contents

    def test_file_exists_error(self, tmp_path: Path) -> None:
        path = tmp_path.joinpath("file.txt")
        with writer(path) as temp1, temp1.open(mode="w") as fh1:
            _ = fh1.write("contents")
        with raises(FileExistsError, match=path.as_posix()), writer(
            path
        ) as temp2, temp2.open(mode="w") as fh2:
            _ = fh2.write("new contents")

    def test_file_overwrite(self, tmp_path: Path) -> None:
        path = tmp_path.joinpath("file.txt")
        with writer(path) as temp1, temp1.open(mode="w") as fh1:
            _ = fh1.write("contents")
        with writer(path, overwrite=True) as temp2, temp2.open(mode="w") as fh2:
            _ = fh2.write("new contents")
        with path.open() as fh3:
            assert fh3.read() == "new contents"

    def test_dir_writing(self, tmp_path: Path) -> None:
        path = tmp_path.joinpath("dir")
        with writer(path) as temp:
            temp.mkdir()
            for i in range(2):
                temp.joinpath(f"file{i}").touch()
        assert len(list(path.iterdir())) == 2

    def test_dir_exists_error(self, tmp_path: Path) -> None:
        path = tmp_path.joinpath("dir")
        with writer(path) as temp1:
            temp1.mkdir()
        with raises(DirectoryExistsError, match=path.as_posix()), writer(path) as temp2:
            temp2.mkdir()

    def test_dir_overwrite(self, tmp_path: Path) -> None:
        path = tmp_path.joinpath("dir")
        with writer(path) as temp1:
            temp1.mkdir()
            for i in range(2):
                temp1.joinpath(f"file{i}").touch()
        with writer(path, overwrite=True) as temp2:
            temp2.mkdir()
            for i in range(3):
                temp2.joinpath(f"file{i}").touch()
        assert len(list(path.iterdir())) == 3

    @mark.parametrize("error", [param(KeyboardInterrupt), param(ValueError)])
    def test_error_during_write(self, tmp_path: Path, error: type[Exception]) -> None:
        path = tmp_path.joinpath("file.txt")

        def raise_error() -> None:
            raise error

        with writer(path) as temp1, temp1.open(mode="w") as fh, suppress(Exception):
            _ = fh.write("contents")
            raise_error()
        expected = int(not issubclass(error, KeyboardInterrupt))
        assert len(list(tmp_path.iterdir())) == expected
