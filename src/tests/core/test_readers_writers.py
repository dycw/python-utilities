from __future__ import annotations

from typing import TYPE_CHECKING

from pytest import mark, param, raises

from utilities._core_errors import (
    ReadBytesNotADirectoryError,
    ReadPickleNotADirectoryError,
)
from utilities.core import (
    ReadBytesFileNotFoundError,
    ReadBytesIsADirectoryError,
    ReadPickleFileNotFoundError,
    ReadPickleIsADirectoryError,
    ReadTextFileNotFoundError,
    ReadTextIsADirectoryError,
    ReadTextNotADirectoryError,
    WriteBytesError,
    WritePickleError,
    WriteTextError,
    is_ci,
    read_bytes,
    read_pickle,
    read_text,
    write_bytes,
    write_pickle,
    write_text,
)

if TYPE_CHECKING:
    from pathlib import Path


class TestReadWriteBytes:
    @mark.parametrize("compress", [param(False), param(True)])
    def test_main(self, *, temp_path_not_exist: Path, compress: bool) -> None:
        write_bytes(temp_path_not_exist, b"data", compress=compress)
        assert temp_path_not_exist.is_file()
        assert read_bytes(temp_path_not_exist, decompress=compress) == b"data"

    def test_json(self, *, temp_path_not_exist: Path) -> None:
        data = b"""{"foo":0,"bar":[1,2,3]}"""
        write_bytes(temp_path_not_exist, data, json=True)
        expected = data if is_ci() else b"""{ "foo": 0, "bar": [1, 2, 3] }\n"""
        assert read_bytes(temp_path_not_exist) == expected

    @mark.parametrize("decompress", [param(False), param(True)])
    def test_error_read_file_not_found(
        self, *, temp_path_not_exist: Path, decompress: bool
    ) -> None:
        with raises(
            ReadBytesFileNotFoundError,
            match=r"Cannot read from '.*' since it does not exist",
        ):
            _ = read_bytes(temp_path_not_exist, decompress=decompress)

    @mark.parametrize("uncompress", [param(False), param(True)])
    def test_error_read_is_a_directory(
        self, *, tmp_path: Path, uncompress: bool
    ) -> None:
        with raises(
            ReadBytesIsADirectoryError,
            match=r"Cannot read from '.*' since it is a directory",
        ):
            _ = read_bytes(tmp_path, decompress=uncompress)

    @mark.parametrize("uncompress", [param(False), param(True)])
    def test_error_read_not_a_directory(
        self, *, temp_path_parent_file: Path, uncompress: bool
    ) -> None:
        with raises(
            ReadBytesNotADirectoryError,
            match=r"Cannot read from '.*' since its parent '.*' is not a directory",
        ):
            _ = read_bytes(temp_path_parent_file, decompress=uncompress)

    def test_error_write(self, *, temp_file: Path) -> None:
        with raises(
            WriteBytesError, match=r"Cannot write to '.*' since it already exists"
        ):
            write_bytes(temp_file, b"data")


class TestReadWritePickle:
    def test_main(self, *, temp_path_not_exist: Path) -> None:
        write_pickle(temp_path_not_exist, None)
        assert temp_path_not_exist.is_file()
        assert read_pickle(temp_path_not_exist) is None

    def test_error_read_file_not_found(self, *, temp_path_not_exist: Path) -> None:
        with raises(
            ReadPickleFileNotFoundError,
            match=r"Cannot read from '.*' since it does not exist",
        ):
            _ = read_pickle(temp_path_not_exist)

    def test_error_read_is_a_directory(self, *, tmp_path: Path) -> None:
        with raises(
            ReadPickleIsADirectoryError,
            match=r"Cannot read from '.*' since it is a directory",
        ):
            _ = read_pickle(tmp_path)

    def test_error_read_not_a_directory(self, *, temp_path_parent_file: Path) -> None:
        with raises(
            ReadPickleNotADirectoryError,
            match=r"Cannot read from '.*' since its parent '.*' is not a directory",
        ):
            _ = read_pickle(temp_path_parent_file)

    def test_error_write(self, *, temp_file: Path) -> None:
        with raises(
            WritePickleError, match=r"Cannot write to '.*' since it already exists"
        ):
            write_pickle(temp_file, b"data")


class TestReadWriteText:
    @mark.parametrize("text", [param("text"), param("text\n")])
    @mark.parametrize("compress", [param(False), param(True)])
    def test_main(
        self, *, temp_path_not_exist: Path, text: str, compress: bool
    ) -> None:
        write_text(temp_path_not_exist, text, compress=compress)
        assert temp_path_not_exist.is_file()
        assert read_text(temp_path_not_exist, decompress=compress) == "text\n"

    @mark.parametrize("decompress", [param(False), param(True)])
    def test_error_read_file_not_found(
        self, *, temp_path_not_exist: Path, decompress: bool
    ) -> None:
        with raises(
            ReadTextFileNotFoundError,
            match=r"Cannot read from '.*' since it does not exist",
        ):
            _ = read_text(temp_path_not_exist, decompress=decompress)

    @mark.parametrize("uncompress", [param(False), param(True)])
    def test_error_read_not_a_directory(
        self, *, temp_path_parent_file: Path, uncompress: bool
    ) -> None:
        with raises(
            ReadTextNotADirectoryError,
            match=r"Cannot read from '.*' since its parent '.*' is not a directory",
        ):
            _ = read_text(temp_path_parent_file, decompress=uncompress)

    @mark.parametrize("uncompress", [param(False), param(True)])
    def test_error_read_is_a_directory(
        self, *, tmp_path: Path, uncompress: bool
    ) -> None:
        with raises(
            ReadTextIsADirectoryError,
            match=r"Cannot read from '.*' since it is a directory",
        ):
            _ = read_text(tmp_path, decompress=uncompress)

    def test_error_write(self, *, temp_file: Path) -> None:
        with raises(
            WriteTextError, match=r"Cannot write to '.*' since it already exists"
        ):
            write_text(temp_file, "text")
