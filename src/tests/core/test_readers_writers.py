from __future__ import annotations

from typing import TYPE_CHECKING

from pytest import mark, param, raises

from utilities.core import WriteBytesError, WriteTextError, write_bytes, write_text

if TYPE_CHECKING:
    from pathlib import Path


class TestWriteBytes:
    def test_main(self, *, temp_path_not_exist: Path) -> None:
        write_bytes(temp_path_not_exist, b"data")
        assert temp_path_not_exist.is_file()
        assert temp_path_not_exist.read_bytes() == b"data"

    def test_error(self, *, temp_file: Path) -> None:
        with raises(
            WriteBytesError, match=r"Cannot write to '.*' since it already exists"
        ):
            write_bytes(temp_file, b"data")


class TestWriteText:
    @mark.parametrize("text", [param("text"), param("text\n")])
    def test_main(self, *, temp_path_not_exist: Path, text: str) -> None:
        write_text(temp_path_not_exist, text)
        assert temp_path_not_exist.is_file()
        assert temp_path_not_exist.read_text() == "text\n"

    def test_error(self, *, temp_file: Path) -> None:
        with raises(
            WriteTextError, match=r"Cannot write to '.*' since it already exists"
        ):
            write_text(temp_file, "text")
