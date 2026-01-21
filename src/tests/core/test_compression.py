from __future__ import annotations

import re
from bz2 import BZ2File
from gzip import GzipFile
from lzma import LZMAFile
from re import DOTALL
from tarfile import ReadError, TarFile
from typing import TYPE_CHECKING, BinaryIO

from pytest import fixture, mark, param, raises

from utilities.core import (
    CompressBZ2Error,
    CompressGzipError,
    CompressLZMAError,
    YieldBZ2Error,
    YieldGzipError,
    YieldLZMAError,
    _compress_files,
    _yield_uncompressed,
    compress_bz2,
    compress_gzip,
    compress_lzma,
    yield_bz2,
    yield_gzip,
    yield_lzma,
)
from utilities.iterables import one

if TYPE_CHECKING:
    from collections.abc import Callable
    from contextlib import AbstractContextManager
    from pathlib import Path

    from _pytest.fixtures import SubRequest

    from utilities.types import PathLike, PathToBinaryIO


@fixture(params=[BZ2File, GzipFile, LZMAFile])
def reader_writer(*, request: SubRequest) -> tuple[PathToBinaryIO, PathToBinaryIO]:
    cls = request.param

    def reader(path: PathLike, /) -> BinaryIO:
        return cls(path, mode="rb")

    def writer(path: PathLike, /) -> BinaryIO:
        return cls(path, mode="wb")

    return reader, writer


@fixture
def reader(*, reader_writer: tuple[PathToBinaryIO, PathToBinaryIO]) -> PathToBinaryIO:
    reader, _ = reader_writer
    return reader


@fixture
def writer(*, reader_writer: tuple[PathToBinaryIO, PathToBinaryIO]) -> PathToBinaryIO:
    _, writer = reader_writer
    return writer


class TestCompressAndYieldUncompressed:
    @mark.parametrize(
        ("compress", "yield_uncompressed"),
        [
            param(compress_bz2, yield_bz2),
            param(compress_gzip, yield_gzip),
            param(compress_lzma, yield_lzma),
        ],
    )
    def test_main(
        self,
        *,
        temp_file: Path,
        temp_path_not_exist: Path,
        compress: Callable[[PathLike, PathLike], None],
        yield_uncompressed: Callable[[PathLike], AbstractContextManager[Path]],
    ) -> None:
        _ = temp_file.write_text("text")
        compress(temp_file, temp_path_not_exist)
        with yield_uncompressed(temp_path_not_exist) as temp:
            assert temp.is_file()
            assert temp.read_text() == "text"

    @mark.parametrize(
        ("yield_uncompressed", "error"),
        [
            param(yield_bz2, YieldBZ2Error),
            param(yield_gzip, YieldGzipError),
            param(yield_lzma, YieldLZMAError),
        ],
    )
    def test_error_read(
        self,
        *,
        temp_path_not_exist: Path,
        yield_uncompressed: Callable[[PathLike], AbstractContextManager[Path]],
        error: type[Exception],
    ) -> None:
        with (
            raises(error, match=r"Cannot uncompress '.*' since it does not exist"),
            yield_uncompressed(temp_path_not_exist),
        ):
            _

    @mark.parametrize(
        ("compress", "error"),
        [
            param(compress_bz2, CompressBZ2Error),
            param(compress_gzip, CompressGzipError),
            param(compress_lzma, CompressLZMAError),
        ],
    )
    def test_error_write(
        self,
        *,
        temp_files: tuple[Path, Path],
        compress: Callable[[PathLike, PathLike], None],
        error: type[Exception],
    ) -> None:
        src, dest = temp_files
        with raises(
            error,
            match=re.compile(
                r"Cannot compress source\(s\) .* since destination '.*' already exists",
                flags=DOTALL,
            ),
        ):
            compress(src, dest)


class TestCompressFiles:
    def test_single_file(
        self,
        *,
        reader: PathToBinaryIO,
        writer: PathToBinaryIO,
        tmp_path: Path,
        temp_file: Path,
    ) -> None:
        _ = temp_file.write_text("text")
        dest = tmp_path / "dest"
        _compress_files(writer, temp_file, dest)
        with reader(dest) as buffer:
            assert buffer.read() == b"text"

    def test_multiple_files(
        self,
        *,
        reader: PathToBinaryIO,
        writer: PathToBinaryIO,
        tmp_path: Path,
        temp_files: tuple[Path, Path],
    ) -> None:
        path1, path2 = temp_files
        dest = tmp_path / "dest"
        _compress_files(writer, path1, path2, dest)
        with reader(dest) as buffer, TarFile(fileobj=buffer) as tar:
            result = set(tar.getnames())
        expected = {p.name for p in temp_files}
        assert result == expected

    def test_single_dir_empty(
        self,
        *,
        reader: PathToBinaryIO,
        writer: PathToBinaryIO,
        tmp_path: Path,
        temp_dir_with_nothing: Path,
    ) -> None:
        dest = tmp_path / "dest"
        _compress_files(writer, temp_dir_with_nothing, dest)
        with reader(dest) as buffer, TarFile(fileobj=buffer) as tar:
            assert tar.getnames() == []

    def test_single_dir_single_file(
        self,
        *,
        reader: PathToBinaryIO,
        writer: PathToBinaryIO,
        tmp_path: Path,
        temp_dir_with_file: Path,
    ) -> None:
        dest = tmp_path / "dest"
        _compress_files(writer, temp_dir_with_file, dest)
        with reader(dest) as buffer, TarFile(fileobj=buffer) as tar:
            result = tar.getnames()
        expected = [one(temp_dir_with_file.iterdir()).name]
        assert result == expected

    def test_single_dir_multiple_files(
        self,
        *,
        reader: PathToBinaryIO,
        writer: PathToBinaryIO,
        tmp_path: Path,
        temp_dir_with_files: Path,
    ) -> None:
        dest = tmp_path / "dest"
        _compress_files(writer, temp_dir_with_files, dest)
        with reader(dest) as buffer, TarFile(fileobj=buffer) as tar:
            result = set(tar.getnames())
        expected = {p.name for p in temp_dir_with_files.iterdir()}
        assert result == expected

    def test_single_dir_nested(
        self,
        *,
        reader: PathToBinaryIO,
        writer: PathToBinaryIO,
        tmp_path: Path,
        temp_dir_with_dir_and_file: Path,
    ) -> None:
        dest = tmp_path / "dest"
        _compress_files(writer, temp_dir_with_dir_and_file, dest)
        with reader(dest) as buffer, TarFile(fileobj=buffer) as tar:
            result = set(tar.getnames())
        inner = one(temp_dir_with_dir_and_file.iterdir())
        expected = {inner.name, f"{inner.name}/{one(inner.iterdir()).name}"}
        assert result == expected

    def test_non_existent(
        self,
        *,
        reader: PathToBinaryIO,
        writer: PathToBinaryIO,
        tmp_path: Path,
        temp_path_not_exist: Path,
    ) -> None:
        dest = tmp_path / "dest"
        _compress_files(writer, temp_path_not_exist, dest)
        with (
            reader(dest) as buffer,
            raises(ReadError, match="empty file"),
            TarFile(fileobj=buffer),
        ):
            ...


class TestYieldUncompressed:
    def test_single_file(
        self,
        *,
        reader: PathToBinaryIO,
        writer: PathToBinaryIO,
        tmp_path: Path,
        temp_file: Path,
    ) -> None:
        _ = temp_file.write_text("text")
        dest = tmp_path / "dest"
        _compress_files(writer, temp_file, dest)
        with _yield_uncompressed(dest, reader) as temp:
            assert temp.is_file()
            assert temp.read_text() == "text"

    def test_multiple_files(
        self,
        *,
        reader: PathToBinaryIO,
        writer: PathToBinaryIO,
        tmp_path: Path,
        temp_files: tuple[Path, Path],
    ) -> None:
        path1, path2 = temp_files
        dest = tmp_path / "dest"
        _compress_files(writer, path1, path2, dest)
        with _yield_uncompressed(dest, reader) as temp:
            assert temp.is_dir()
            result = {p.name for p in temp.iterdir()}
            expected = {p.name for p in temp_files}
            assert result == expected

    def test_single_dir_empty(
        self,
        *,
        reader: PathToBinaryIO,
        writer: PathToBinaryIO,
        tmp_path: Path,
        temp_dir_with_nothing: Path,
    ) -> None:
        dest = tmp_path / "dest"
        _compress_files(writer, temp_dir_with_nothing, dest)
        with _yield_uncompressed(dest, reader) as temp:
            assert temp.is_dir()
            assert list(temp.iterdir()) == []

    def test_single_dir_single_file(
        self,
        *,
        reader: PathToBinaryIO,
        writer: PathToBinaryIO,
        tmp_path: Path,
        temp_dir_with_file: Path,
    ) -> None:
        dest = tmp_path / "dest"
        _compress_files(writer, temp_dir_with_file, dest)
        with _yield_uncompressed(dest, reader) as temp:
            assert temp.is_file()
            expected = one(temp_dir_with_file.iterdir()).name
            assert temp.name == expected

    def test_single_dir_multiple_files(
        self,
        *,
        reader: PathToBinaryIO,
        writer: PathToBinaryIO,
        tmp_path: Path,
        temp_dir_with_files: Path,
    ) -> None:
        dest = tmp_path / "dest"
        _compress_files(writer, temp_dir_with_files, dest)
        with _yield_uncompressed(dest, reader) as temp:
            assert temp.is_dir()
            result = {p.name for p in temp.iterdir()}
            expected = {p.name for p in temp_dir_with_files.iterdir()}
            assert result == expected

    def test_single_dir_nested(
        self,
        *,
        reader: PathToBinaryIO,
        writer: PathToBinaryIO,
        tmp_path: Path,
        temp_dir_with_dir_and_file: Path,
    ) -> None:
        dest = tmp_path / "dest"
        _compress_files(writer, temp_dir_with_dir_and_file, dest)
        with _yield_uncompressed(dest, reader) as temp:
            assert temp.is_dir()
            assert one(temp.iterdir()).is_file()

    def test_multiple_dirs(
        self,
        *,
        reader: PathToBinaryIO,
        writer: PathToBinaryIO,
        tmp_path: Path,
        temp_dirs_with_files: tuple[Path, Path],
    ) -> None:
        path1, path2 = temp_dirs_with_files
        dest = tmp_path / "dest"
        _compress_files(writer, path1, path2, dest)
        with _yield_uncompressed(dest, reader) as temp:
            result = {p.name for p in temp.iterdir()}
        expected = {one(path1.iterdir()).name, one(path2.iterdir()).name}
        assert result == expected

    def test_single_non_existent(
        self,
        *,
        reader: PathToBinaryIO,
        writer: PathToBinaryIO,
        tmp_path: Path,
        temp_path_not_exist: Path,
    ) -> None:
        dest = tmp_path / "dest"
        _compress_files(writer, temp_path_not_exist, dest)
        with _yield_uncompressed(dest, reader) as temp:
            assert temp.is_dir()
            assert list(temp.iterdir()) == []

    def test_multiple_non_existent(
        self,
        *,
        reader: PathToBinaryIO,
        writer: PathToBinaryIO,
        tmp_path: Path,
        temp_path_not_exist: Path,
    ) -> None:
        dest = tmp_path / "dest"
        _compress_files(writer, temp_path_not_exist, temp_path_not_exist, dest)
        with _yield_uncompressed(dest, reader) as temp:
            assert temp.is_dir()
            assert list(temp.iterdir()) == []
