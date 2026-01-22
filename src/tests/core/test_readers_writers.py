from __future__ import annotations

from typing import TYPE_CHECKING

from pytest import mark, param, raises

from utilities.constants import IS_CI
from utilities.core import (
    ReadBytesError,
    ReadPickleError,
    ReadTextError,
    WriteBytesError,
    WritePickleError,
    WriteTextError,
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
        expected = data if IS_CI else b"""{ "foo": 0, "bar": [1, 2, 3] }\n"""
        assert read_bytes(temp_path_not_exist) == expected

    @mark.parametrize("uncompress", [param(False), param(True)])
    def test_error_read(self, *, temp_path_not_exist: Path, uncompress: bool) -> None:
        with raises(
            ReadBytesError, match=r"Cannot read from '.*' since it does not exist"
        ):
            _ = read_bytes(temp_path_not_exist, decompress=uncompress)

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

    def test_error_read(self, *, temp_path_not_exist: Path) -> None:
        with raises(
            ReadPickleError, match=r"Cannot read from '.*' since it does not exist"
        ):
            _ = read_pickle(temp_path_not_exist)

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

    @mark.parametrize("uncompress", [param(False), param(True)])
    def test_error_read(self, *, temp_path_not_exist: Path, uncompress: bool) -> None:
        with raises(
            ReadTextError, match=r"Cannot read from '.*' since it does not exist"
        ):
            _ = read_text(temp_path_not_exist, decompress=uncompress)

    def test_error_write(self, *, temp_file: Path) -> None:
        with raises(
            WriteTextError, match=r"Cannot write to '.*' since it already exists"
        ):
            write_text(temp_file, "text")
