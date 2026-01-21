from __future__ import annotations

from typing import TYPE_CHECKING

from pytest import mark, param, raises

from utilities.core import (
    YieldWritePathError,
    write_bytes,
    write_text,
    yield_gzip,
    yield_write_path,
)

if TYPE_CHECKING:
    from pathlib import Path


class TestWriteBytes:
    def test_main(self, *, temp_path_not_exist: Path) -> None:
        write_bytes(temp_path_not_exist, b"data")
        assert temp_path_not_exist.is_file()
        assert temp_path_not_exist.read_bytes() == b"data"


class TestWriteText:
    @mark.parametrize("text", [param("text"), param("text\n")])
    def test_main(self, *, temp_path_not_exist: Path, text: str) -> None:
        write_text(temp_path_not_exist, text)
        assert temp_path_not_exist.is_file()
        assert temp_path_not_exist.read_text() == "text\n"


class TestYieldWritePath:
    def test_main(self, *, temp_path_not_exist: Path) -> None:
        with yield_write_path(temp_path_not_exist) as temp:
            _ = temp.write_text("text")
            assert not temp_path_not_exist.exists()
        assert temp_path_not_exist.is_file()
        assert temp_path_not_exist.read_text() == "text"

    def test_compress(self, *, temp_path_not_exist: Path) -> None:
        with yield_write_path(temp_path_not_exist, compress=True) as temp:
            _ = temp.write_bytes(b"data")
        assert temp_path_not_exist.is_file()
        with yield_gzip(temp_path_not_exist) as temp:
            assert temp.read_bytes() == b"data"

    def test_overwrite(self, *, temp_file: Path) -> None:
        _ = temp_file.write_text("init")
        with yield_write_path(temp_file, overwrite=True) as temp:
            _ = temp.write_text("post")
        assert temp_file.read_text() == "post"

    @mark.parametrize("compress", [param(False), param(True, marks=mark.xfail)])
    def test_error(self, *, temp_file: Path, compress: bool) -> None:
        with (
            raises(
                YieldWritePathError,
                match=r"Cannot write to '.*' since it already exists",
            ),
            yield_write_path(temp_file, compress=compress),
        ):
            ...
