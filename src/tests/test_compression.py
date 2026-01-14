from __future__ import annotations

from bz2 import BZ2File
from gzip import GzipFile
from lzma import LZMAFile
from tarfile import ReadError, TarFile
from typing import TYPE_CHECKING, BinaryIO, cast

from hypothesis import given
from hypothesis.strategies import binary, booleans
from pytest import fixture, mark, raises

from utilities.compression import compress_paths, yield_compressed_contents
from utilities.gzip import gzip_paths, read_binary, write_binary, yield_gzip_contents
from utilities.hypothesis import temp_paths
from utilities.iterables import one

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

    from _pytest.fixtures import RequestFixtureDef, SubRequest

    from utilities.types import PathLike, PathToBinaryIO


@fixture(params=[(BZ2File, True), (GzipFile, True), (LZMAFile, True)])
def reader_writer(*, request: SubRequest) -> tuple[PathToBinaryIO, PathToBinaryIO]:
    cls, add_r = request.param

    def reader(path: PathLike, /) -> BinaryIO:
        mode = ("r" if add_r else "") + "b"
        return cls(path, mode="rb")

    def writer(path: PathLike, /) -> BinaryIO:
        mode = ("w" if add_r else "") + "b"
        return cls(path, mode="wb")

    return reader, writer


@fixture(params=[BZ2File, GzipFile, LZMAFile])
def writer(*, reader_writer: tuple[PathToBinaryIO, PathToBinaryIO]) -> PathToBinaryIO:
    _, writer = reader_writer
    return writer


class TestGzipPath:
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

    def test_dir_empty(self, tmp_path: Path, temp_dir_with_nothing: Path) -> None:
        dest = tmp_path / "gzip-tar"
        gzip_paths(temp_dir_with_nothing, dest)
        with GzipFile(dest) as gz, TarFile(fileobj=gz) as tar:
            assert tar.getnames() == []

    def test_dir_single_file(self, tmp_path: Path, temp_dir_with_file: Path) -> None:
        dest = tmp_path / "zip"
        gzip_paths(temp_dir_with_file, dest)
        with GzipFile(dest) as gz, TarFile(fileobj=gz) as tar:
            result = tar.getnames()
        expected = [one(temp_dir_with_file.iterdir()).name]
        assert result == expected

    def test_dir_multiple_files(
        self, tmp_path: Path, temp_dir_with_files: Path
    ) -> None:
        dest = tmp_path / "zip"
        gzip_paths(temp_dir_with_files, dest)
        with GzipFile(dest) as gz, TarFile(fileobj=gz) as tar:
            result = set(tar.getnames())
        expected = {p.name for p in temp_dir_with_files.iterdir()}
        assert result == expected

    def test_dir_nested(self, tmp_path: Path, temp_dir_with_dir_and_file: Path) -> None:
        dest = tmp_path / "zip"
        gzip_paths(temp_dir_with_dir_and_file, dest)
        with GzipFile(dest) as gz, TarFile(fileobj=gz) as tar:
            result = set(tar.getnames())
        inner = one(temp_dir_with_dir_and_file.iterdir())
        expected = {inner.name, f"{inner.name}/{one(inner.iterdir()).name}"}
        assert result == expected

    def test_non_existent(self, tmp_path: Path, temp_path_not_exist: Path) -> None:
        dest = tmp_path / "zip"
        gzip_paths(temp_path_not_exist, dest)
        with (
            GzipFile(dest) as gz,
            raises(ReadError, match="empty file"),
            TarFile(fileobj=gz),
        ):
            ...


class TestYieldCompressedContents:
    def test_single_file(
        self,
        *,
        reader_writer: tuple[PathToBinaryIO, PathToBinaryIO],
        tmp_path: Path,
        temp_file: Path,
    ) -> None:
        reader, writer = reader_writer
        _ = temp_file.write_text("text")
        dest = tmp_path / "dest"
        compress_paths(writer, temp_file, dest)
        with yield_compressed_contents(dest, reader) as temp:
            assert temp.is_file()
            assert temp.read_text() == "text"

    def test_multiple_files(
        self,
        *,
        reader_writer: tuple[PathToBinaryIO, PathToBinaryIO],
        tmp_path: Path,
        temp_files: tuple[Path, Path],
    ) -> None:
        reader, writer = reader_writer
        path1, path2 = temp_files
        dest = tmp_path / "gzip-tar"
        compress_paths(writer, path1, path2, dest)
        with yield_compressed_contents(dest, reader) as temp:
            assert temp.is_dir()
            result = {p.name for p in temp.iterdir()}
            expected = {p.name for p in temp_files}
            assert result == expected

    def test_dir_empty(self, tmp_path: Path, temp_dir_with_nothing: Path) -> None:
        dest = tmp_path / "gzip-tar"
        gzip_paths(temp_dir_with_nothing, dest)
        with yield_gzip_contents(dest) as temp:
            assert temp.is_dir()
            assert list(temp.iterdir()) == []

    def test_dir_single_file(self, tmp_path: Path, temp_dir_with_file: Path) -> None:
        dest = tmp_path / "zip"
        gzip_paths(temp_dir_with_file, dest)
        with yield_gzip_contents(dest) as temp:
            assert temp.is_file()
            expected = one(temp_dir_with_file.iterdir()).name
            assert temp.name == expected

    def test_dir_multiple_files(
        self, tmp_path: Path, temp_dir_with_files: Path
    ) -> None:
        dest = tmp_path / "zip"
        gzip_paths(temp_dir_with_files, dest)
        with yield_gzip_contents(dest) as temp:
            assert temp.is_dir()
            result = {p.name for p in temp.iterdir()}
            expected = {p.name for p in temp_dir_with_files.iterdir()}
            assert result == expected

    def test_dir_nested(self, tmp_path: Path, temp_dir_with_dir_and_file: Path) -> None:
        dest = tmp_path / "zip"
        gzip_paths(temp_dir_with_dir_and_file, dest)
        with yield_gzip_contents(dest) as temp:
            assert temp.is_dir()
            assert one(temp.iterdir()).is_file()

    def test_non_existent(self, tmp_path: Path, temp_path_not_exist: Path) -> None:
        dest = tmp_path / "zip"
        gzip_paths(temp_path_not_exist, dest)
        with yield_gzip_contents(dest) as temp:
            assert temp.is_dir()
            assert list(temp.iterdir()) == []
