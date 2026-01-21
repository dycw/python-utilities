from __future__ import annotations

from typing import TYPE_CHECKING

from pytest import mark, param, raises

from utilities.core import (
    WriteBytesError,
    WriteTextError,
    read_bytes,
    read_text,
    write_bytes,
    write_text,
)

if TYPE_CHECKING:
    from pathlib import Path


class TestReadWriteBytes:
    @mark.parametrize("compress", [param(False), param(True)])
    def test_main(self, *, temp_path_not_exist: Path, compress: bool) -> None:
        write_bytes(temp_path_not_exist, b"data", compress=compress)
        assert temp_path_not_exist.is_file()
        assert read_bytes(temp_path_not_exist, uncompress=compress) == b"data"

    def test_error(self, *, temp_file: Path) -> None:
        with raises(
            WriteBytesError, match=r"Cannot write to '.*' since it already exists"
        ):
            write_bytes(temp_file, b"data")


class TestWriteText:
    @mark.parametrize("text", [param("text"), param("text\n")])
    @mark.parametrize("compress", [param(False), param(True)])
    def test_main(
        self, *, temp_path_not_exist: Path, text: str, compress: bool
    ) -> None:
        write_text(temp_path_not_exist, text, compress=compress)
        assert temp_path_not_exist.is_file()
        assert read_text(temp_path_not_exist, uncompress=compress) == "text\n"

    def test_error(self, *, temp_file: Path) -> None:
        with raises(
            WriteTextError, match=r"Cannot write to '.*' since it already exists"
        ):
            write_text(temp_file, "text")
