from __future__ import annotations

import re
from bz2 import BZ2File
from gzip import GzipFile
from lzma import LZMAFile
from re import DOTALL
from tarfile import ReadError, TarFile
from typing import TYPE_CHECKING, BinaryIO
from zipfile import ZipFile

from pytest import fixture, raises

from utilities.core import (
    CompressBZ2Error,
    CompressGzipError,
    CompressLZMAError,
    CompressZipError,
    YieldBZ2Error,
    YieldGzipError,
    YieldLZMAError,
    YieldZipError,
    _compress_files,
    _yield_uncompressed,
    compress_bz2,
    compress_gzip,
    compress_lzma,
    compress_zip,
    yield_bz2,
    yield_gzip,
    yield_lzma,
    yield_zip,
)
from utilities.iterables import one

if TYPE_CHECKING:
    from collections.abc import Callable
    from contextlib import AbstractContextManager
    from pathlib import Path

    from _pytest.fixtures import SubRequest

    from utilities.types import PathLike, PathToBinaryIO


# bz2, gzip, lzma - fixtures


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


type Compress = Callable[..., None]
type YieldUncompressed = Callable[[PathLike], AbstractContextManager[Path]]
type Data = tuple[Compress, YieldUncompressed, type[Exception], type[Exception]]


@fixture(
    params=[
        (compress_bz2, yield_bz2, CompressBZ2Error, YieldBZ2Error),
        (compress_gzip, yield_gzip, CompressGzipError, YieldGzipError),
        (compress_lzma, yield_lzma, CompressLZMAError, YieldLZMAError),
        (compress_zip, yield_zip, CompressZipError, YieldZipError),
    ]
)
def data(*, request: SubRequest) -> Data:
    return request.param


@fixture
def compress_new(*, data: Data) -> Compress:
    compress, _, _, _ = data
    return compress


@fixture
def yield_uncompressed_new(*, data: Data) -> YieldUncompressed:
    _, yield_uncompressed, _, _ = data
    return yield_uncompressed


@fixture
def error_compress(*, data: Data) -> type[Exception]:
    _, _, error, _ = data
    return error


@fixture
def error_yield_uncompressed(*, data: Data) -> type[Exception]:
    _, _, _, error = data
    return error


# all


class TestCompressAndYieldUncompressed:
    def test_single_file(
        self,
        *,
        temp_file: Path,
        temp_path_not_exist: Path,
        compress_new: Compress,
        yield_uncompressed_new: YieldUncompressed,
    ) -> None:
        _ = temp_file.write_text("text")
        compress_new(temp_file, temp_path_not_exist)
        with yield_uncompressed_new(temp_path_not_exist) as temp:
            assert temp.is_file()
            assert temp.read_text() == "text"

    def test_multiple_files(
        self,
        *,
        temp_files: tuple[Path, Path],
        temp_path_not_exist: Path,
        compress_new: Compress,
        yield_uncompressed_new: YieldUncompressed,
    ) -> None:
        path1, path2 = temp_files
        compress_new(path1, path2, temp_path_not_exist)
        with yield_uncompressed_new(temp_path_not_exist) as temp:
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
        with reader(dest) as buffer, TarFile(fileobj=buffer) as tar:
            assert tar.getnames() == []

    def test_error_compress(
        self,
        *,
        temp_files: tuple[Path, Path],
        compress_new: Compress,
        error_compress: type[Exception],
    ) -> None:
        src, dest = temp_files
        with raises(
            error_compress,
            match=re.compile(
                r"Cannot compress source\(s\) .* since destination '.*' already exists",
                flags=DOTALL,
            ),
        ):
            compress_new(src, dest)

    def test_error_yield_uncompressed(
        self,
        *,
        temp_path_not_exist: Path,
        yield_uncompressed_new: YieldUncompressed,
        error_yield_uncompressed: type[Exception],
    ) -> None:
        with (
            raises(
                error_yield_uncompressed,
                match=r"Cannot uncompress '.*' since it does not exist",
            ),
            yield_uncompressed_new(temp_path_not_exist),
        ):
            ...


class TestCompressFiles:
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


# zip


class TestCompressZip:
    def test_single_file(self, tmp_path: Path, temp_file: Path) -> None:
        dest = tmp_path / "zip"
        compress_zip(temp_file, dest)
        with ZipFile(dest) as zf:
            assert zf.namelist() == [temp_file.name]

    def test_multiple_files(
        self, tmp_path: Path, temp_files: tuple[Path, Path]
    ) -> None:
        path1, path2 = temp_files
        dest = tmp_path / "zip"
        compress_zip(path1, path2, dest)
        with ZipFile(dest) as zf:
            assert set(zf.namelist()) == {p.name for p in temp_files}

    def test_dir_empty(self, tmp_path: Path, temp_dir_with_nothing: Path) -> None:
        dest = tmp_path / "zip"
        compress_zip(temp_dir_with_nothing, dest)
        with ZipFile(dest) as zf:
            assert zf.namelist() == []

    def test_dir_single_file(self, tmp_path: Path, temp_dir_with_file: Path) -> None:
        dest = tmp_path / "zip"
        compress_zip(temp_dir_with_file, dest)
        with ZipFile(dest) as zf:
            result = zf.namelist()
        expected = [one(temp_dir_with_file.iterdir()).name]
        assert result == expected

    def test_dir_multiple_files(
        self, tmp_path: Path, temp_dir_with_files: Path
    ) -> None:
        dest = tmp_path / "zip"
        compress_zip(temp_dir_with_files, dest)
        with ZipFile(dest) as zf:
            result = set(zf.namelist())
        expected = {p.name for p in temp_dir_with_files.iterdir()}
        assert result == expected

    def test_dir_nested(self, tmp_path: Path, temp_dir_with_dir_and_file: Path) -> None:
        dest = tmp_path / "zip"
        compress_zip(temp_dir_with_dir_and_file, dest)
        with ZipFile(dest) as zf:
            result = list(zf.namelist())
        inner = one(temp_dir_with_dir_and_file.iterdir())
        expected = [f"{inner.name}/", f"{inner.name}/{one(inner.iterdir()).name}"]
        assert result == expected

    def test_non_existent(self, tmp_path: Path, temp_path_not_exist: Path) -> None:
        dest = tmp_path / "zip"
        compress_zip(temp_path_not_exist, dest)
        with ZipFile(dest) as zf:
            assert zf.namelist() == []


class TestYieldZipFileContents:
    def test_single_file(self, tmp_path: Path, temp_file: Path) -> None:
        dest = tmp_path / "zip"
        compress_zip(temp_file, dest)
        with yield_zip(dest) as temp:
            assert temp.is_file()
            assert temp.name == temp_file.name

    def test_multiple_files(
        self, tmp_path: Path, temp_files: tuple[Path, Path]
    ) -> None:
        path1, path2 = temp_files
        dest = tmp_path / "zip"
        compress_zip(path1, path2, dest)
        with yield_zip(dest) as temp:
            assert temp.is_dir()
            result = {p.name for p in temp.iterdir()}
            expected = {p.name for p in temp_files}
            assert result == expected

    def test_dir_empty(self, tmp_path: Path, temp_dir_with_nothing: Path) -> None:
        dest = tmp_path / "zip"
        compress_zip(temp_dir_with_nothing, dest)
        with yield_zip(dest) as temp:
            assert temp.is_dir()
            assert list(temp.iterdir()) == []

    def test_dir_single_file(self, tmp_path: Path, temp_dir_with_file: Path) -> None:
        dest = tmp_path / "zip"
        compress_zip(temp_dir_with_file, dest)
        with yield_zip(dest) as temp:
            assert temp.is_file()
            expected = one(temp_dir_with_file.iterdir()).name
            assert temp.name == expected

    def test_dir_multiple_files(
        self, tmp_path: Path, temp_dir_with_files: Path
    ) -> None:
        dest = tmp_path / "zip"
        compress_zip(temp_dir_with_files, dest)
        with yield_zip(dest) as temp:
            assert temp.is_dir()
            result = {p.name for p in temp.iterdir()}
            expected = {p.name for p in temp_dir_with_files.iterdir()}
            assert result == expected

    def test_dir_nested(self, tmp_path: Path, temp_dir_with_dir_and_file: Path) -> None:
        dest = tmp_path / "zip"
        compress_zip(temp_dir_with_dir_and_file, dest)
        with yield_zip(dest) as temp:
            assert temp.is_dir()
            assert one(temp.iterdir()).is_file()

    def test_non_existent(self, tmp_path: Path, temp_path_not_exist: Path) -> None:
        dest = tmp_path / "zip"
        compress_zip(temp_path_not_exist, dest)
        with yield_zip(dest) as temp:
            assert temp.is_dir()
            assert list(temp.iterdir()) == []
