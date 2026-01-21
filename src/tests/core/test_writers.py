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
    def test_main(self, *, tmp_path: Path) -> None:
        path = tmp_path / "file"
        write_bytes(path, b"data")
        assert path.is_file()
        assert path.read_bytes() == b"data"


class TestWriteText:
    @mark.parametrize("text", [param("text"), param("text\n")])
    def test_main(self, *, tmp_path: Path, text: str) -> None:
        path = tmp_path / "file.txt"
        write_text(path, text)
        assert path.is_file()
        assert path.read_text() == "text\n"


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

    def test_error(self, *, temp_file: Path) -> None:
        with (
            raises(
                YieldWritePathError,
                match=r"Cannot write to '.*' since it already exists",
            ),
            yield_write_path(temp_file),
        ):
            ...
