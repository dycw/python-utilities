from __future__ import annotations

from gzip import GzipFile
from tarfile import TarFile
from typing import TYPE_CHECKING

from hypothesis import given
from hypothesis.strategies import binary, booleans
from pytest import mark

from utilities.gzip import gzip_paths, read_binary, write_binary
from utilities.hypothesis import temp_paths

if TYPE_CHECKING:
    from pathlib import Path


class TestGZipPath:
    def test_single_file(self, tmp_path: Path, temp_file: Path) -> None:
        _ = temp_file.write_text("text")
        dest = tmp_path / "gzip"
        gzip_paths(temp_file, dest)
        with GzipFile(dest) as gz:
            assert gz.read() == b"text"

    def test_multiple_files(
        self, tmp_path: Path, temp_files: tuple[Path, Path]
    ) -> None:
        path1, path2 = temp_files
        dest = tmp_path / "gzip-tar"
        gzip_paths(path1, path2, dest)
        with GzipFile(dest) as gz, TarFile(fileobj=gz) as tar:
            result = set(tar.getnames())
        expected = {p.name for p in temp_files}
        assert result == expected

    @mark.only
    def test_dir_empty(self, tmp_path: Path) -> None:
        src = tmp_path / "src"
        src.mkdir()
        dest = tmp_path / "gzip-tar"
        gzip_paths(src, dest)
        with GzipFile(dest) as gz, TarFile(fileobj=gz) as tar:
            assert tar.getnames() == []


class TestReadAndWriteBinary:
    @given(root=temp_paths(), data=binary(), compress=booleans())
    def test_main(self, *, root: Path, data: bytes, compress: bool) -> None:
        file = root.joinpath("file.json")
        write_binary(data, file, compress=compress)
        contents = read_binary(file, decompress=compress)
        assert contents == data
