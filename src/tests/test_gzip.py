from __future__ import annotations

from typing import TYPE_CHECKING

from hypothesis import given
from hypothesis.strategies import binary, booleans

from utilities.gzip import gzip_paths, read_binary, write_binary, yield_gzip_contents
from utilities.hypothesis import temp_paths

if TYPE_CHECKING:
    from pathlib import Path


class TestReadAndWriteBinary:
    @given(root=temp_paths(), data=binary(), compress=booleans())
    def test_main(self, *, root: Path, data: bytes, compress: bool) -> None:
        file = root.joinpath("file.json")
        write_binary(data, file, compress=compress)
        contents = read_binary(file, decompress=compress)
        assert contents == data


class TestGzipPathsAndYieldGzipContents:
    def test_single_file(self, *, tmp_path: Path, temp_file: Path) -> None:
        _ = temp_file.write_text("text")
        dest = tmp_path / "dest"
        gzip_paths(temp_file, dest)
        with yield_gzip_contents(dest) as temp:
            assert temp.is_file()
            assert temp.read_text() == "text"
