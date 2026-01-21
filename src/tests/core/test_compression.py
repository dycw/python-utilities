from __future__ import annotations

import re
from bz2 import BZ2File
from gzip import GzipFile
from lzma import LZMAFile
from re import DOTALL
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
        _ = path1.write_text("text1")
        _ = path2.write_text("text2")
        compress_new(path1, path2, temp_path_not_exist)
        with yield_uncompressed_new(temp_path_not_exist) as temp:
            assert temp.is_dir()
            assert len(list(temp.iterdir())) == 2
            assert (temp / path1.name).is_file()
            assert (temp / path1.name).read_text() == "text1"
            assert (temp / path2.name).is_file()
            assert (temp / path2.name).read_text() == "text2"

    def test_single_dir_empty(
        self,
        *,
        temp_dir_with_nothing: Path,
        temp_path_not_exist: Path,
        compress_new: Compress,
        yield_uncompressed_new: YieldUncompressed,
    ) -> None:
        compress_new(temp_dir_with_nothing, temp_path_not_exist)
        with yield_uncompressed_new(temp_path_not_exist) as temp:
            assert temp.is_dir()
            assert len(list(temp.iterdir())) == 0

    def test_single_dir_single_file(
        self,
        *,
        temp_dir_with_file: Path,
        temp_path_not_exist: Path,
        compress_new: Compress,
        yield_uncompressed_new: YieldUncompressed,
    ) -> None:
        inner = one(temp_dir_with_file.iterdir())
        _ = inner.write_text("text")
        compress_new(temp_dir_with_file, temp_path_not_exist)
        with yield_uncompressed_new(temp_path_not_exist) as temp:
            assert temp.is_file()
            assert temp.read_text() == "text"

    def test_single_dir_multiple_files(
        self,
        *,
        temp_dir_with_files: Path,
        temp_path_not_exist: Path,
        compress_new: Compress,
        yield_uncompressed_new: YieldUncompressed,
    ) -> None:
        path1, path2 = sorted(temp_dir_with_files.iterdir())
        _ = path1.write_text("text1")
        _ = path2.write_text("text2")
        compress_new(temp_dir_with_files, temp_path_not_exist)
        with yield_uncompressed_new(temp_path_not_exist) as temp:
            assert temp.is_dir()
            assert len(list(temp.iterdir())) == 2
            assert (temp / path1.name).is_file()
            assert (temp / path1.name).read_text() == "text1"
            assert (temp / path2.name).is_file()
            assert (temp / path2.name).read_text() == "text2"

    def test_single_dir_nested(
        self,
        *,
        temp_dir_with_dir_and_file: Path,
        temp_path_not_exist: Path,
        compress_new: Compress,
        yield_uncompressed_new: YieldUncompressed,
    ) -> None:
        inner_dir = one(temp_dir_with_dir_and_file.iterdir())
        inner_file = one(inner_dir.iterdir())
        _ = inner_file.write_text("text")
        compress_new(temp_dir_with_dir_and_file, temp_path_not_exist)
        with yield_uncompressed_new(temp_path_not_exist) as temp:
            assert temp.is_dir()
            assert len(list(temp.iterdir())) == 1
            assert (temp / inner_file.name).is_file()
            assert (temp / inner_file.name).read_text() == "text"

    def test_multiple_dirs(
        self,
        *,
        temp_dirs_with_files: tuple[Path, Path],
        temp_path_not_exist: Path,
        compress_new: Compress,
        yield_uncompressed_new: YieldUncompressed,
    ) -> None:
        dir1, dir2 = temp_dirs_with_files
        file1 = one(dir1.iterdir())
        _ = file1.write_text("text1")
        file2 = one(dir2.iterdir())
        _ = file2.write_text("text2")
        compress_new(dir1, dir2, temp_path_not_exist)
        with yield_uncompressed_new(temp_path_not_exist) as temp:
            assert temp.is_dir()
            assert len(list(temp.iterdir())) == 2
            assert (temp / file1.name).is_file()
            assert (temp / file1.name).read_text() == "text1"
            assert (temp / file2.name).is_file()
            assert (temp / file2.name).read_text() == "text2"

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


if 0:

    class TestYieldUncompressed:
        def test_single_non_existent(
            self,
            *,
            compress_new: Compress,
            yield_uncompressed_new: YieldUncompressed,
            tmp_path: Path,
            temp_path_not_exist: Path,
        ) -> None:
            temp_path_not_exist = tmp_path / "temp_path_not_exist"
            _compress_files(writer, temp_path_not_exist, temp_path_not_exist)
            with _yield_uncompressed(temp_path_not_exist, reader) as temp:
                assert temp.is_dir()
                assert list(temp.iterdir()) == []

        def test_multiple_non_existent(
            self,
            *,
            tmp_path: Path,
            temp_path_not_exist: Path,
            compress_new: Compress,
            yield_uncompressed_new: YieldUncompressed,
        ) -> None:
            temp_path_not_exist = tmp_path / "temp_path_not_exist"
            _compress_files(
                writer, temp_path_not_exist, temp_path_not_exist, temp_path_not_exist
            )
            with _yield_uncompressed(temp_path_not_exist, reader) as temp:
                assert temp.is_dir()
                assert list(temp.iterdir()) == []

    # zip

    class TestCompressZip:
        def test_single_file(self, tmp_path: Path, temp_file: Path) -> None:
            temp_path_not_exist = tmp_path / "zip"
            compress_zip(temp_file, temp_path_not_exist)
            with ZipFile(temp_path_not_exist) as zf:
                assert zf.namelist() == [temp_file.name]

        def test_multiple_files(
            self, tmp_path: Path, temp_files: tuple[Path, Path]
        ) -> None:
            path1, path2 = temp_files
            temp_path_not_exist = tmp_path / "zip"
            compress_zip(path1, path2, temp_path_not_exist)
            with ZipFile(temp_path_not_exist) as zf:
                assert set(zf.namelist()) == {p.name for p in temp_files}

        def test_dir_empty(self, tmp_path: Path, temp_dir_with_nothing: Path) -> None:
            temp_path_not_exist = tmp_path / "zip"
            compress_zip(temp_dir_with_nothing, temp_path_not_exist)
            with ZipFile(temp_path_not_exist) as zf:
                assert zf.namelist() == []

        def test_dir_single_file(
            self, tmp_path: Path, temp_dir_with_file: Path
        ) -> None:
            temp_path_not_exist = tmp_path / "zip"
            compress_zip(temp_dir_with_file, temp_path_not_exist)
            with ZipFile(temp_path_not_exist) as zf:
                result = zf.namelist()
            expected = [one(temp_dir_with_file.iterdir()).name]
            assert result == expected

        def test_dir_multiple_files(
            self, tmp_path: Path, temp_dir_with_files: Path
        ) -> None:
            temp_path_not_exist = tmp_path / "zip"
            compress_zip(temp_dir_with_files, temp_path_not_exist)
            with ZipFile(temp_path_not_exist) as zf:
                result = set(zf.namelist())
            expected = {p.name for p in temp_dir_with_files.iterdir()}
            assert result == expected

        def test_dir_nested(
            self, tmp_path: Path, temp_dir_with_dir_and_file: Path
        ) -> None:
            temp_path_not_exist = tmp_path / "zip"
            compress_zip(temp_dir_with_dir_and_file, temp_path_not_exist)
            with ZipFile(temp_path_not_exist) as zf:
                result = list(zf.namelist())
            inner = one(temp_dir_with_dir_and_file.iterdir())
            expected = [f"{inner.name}/", f"{inner.name}/{one(inner.iterdir()).name}"]
            assert result == expected

        def test_non_existent(self, tmp_path: Path, temp_path_not_exist: Path) -> None:
            temp_path_not_exist = tmp_path / "zip"
            compress_zip(temp_path_not_exist, temp_path_not_exist)
            with ZipFile(temp_path_not_exist) as zf:
                assert zf.namelist() == []

    class TestYieldZipFileContents:
        def test_single_file(self, tmp_path: Path, temp_file: Path) -> None:
            temp_path_not_exist = tmp_path / "zip"
            compress_zip(temp_file, temp_path_not_exist)
            with yield_zip(temp_path_not_exist) as temp:
                assert temp.is_file()
                assert temp.name == temp_file.name

        def test_multiple_files(
            self, tmp_path: Path, temp_files: tuple[Path, Path]
        ) -> None:
            path1, path2 = temp_files
            temp_path_not_exist = tmp_path / "zip"
            compress_zip(path1, path2, temp_path_not_exist)
            with yield_zip(temp_path_not_exist) as temp:
                assert temp.is_dir()
                result = {p.name for p in temp.iterdir()}
                expected = {p.name for p in temp_files}
                assert result == expected

        def test_dir_empty(self, tmp_path: Path, temp_dir_with_nothing: Path) -> None:
            temp_path_not_exist = tmp_path / "zip"
            compress_zip(temp_dir_with_nothing, temp_path_not_exist)
            with yield_zip(temp_path_not_exist) as temp:
                assert temp.is_dir()
                assert list(temp.iterdir()) == []

        def test_dir_single_file(
            self, tmp_path: Path, temp_dir_with_file: Path
        ) -> None:
            temp_path_not_exist = tmp_path / "zip"
            compress_zip(temp_dir_with_file, temp_path_not_exist)
            with yield_zip(temp_path_not_exist) as temp:
                assert temp.is_file()
                expected = one(temp_dir_with_file.iterdir()).name
                assert temp.name == expected

        def test_dir_multiple_files(
            self, tmp_path: Path, temp_dir_with_files: Path
        ) -> None:
            temp_path_not_exist = tmp_path / "zip"
            compress_zip(temp_dir_with_files, temp_path_not_exist)
            with yield_zip(temp_path_not_exist) as temp:
                assert temp.is_dir()
                result = {p.name for p in temp.iterdir()}
                expected = {p.name for p in temp_dir_with_files.iterdir()}
                assert result == expected

        def test_dir_nested(
            self, tmp_path: Path, temp_dir_with_dir_and_file: Path
        ) -> None:
            temp_path_not_exist = tmp_path / "zip"
            compress_zip(temp_dir_with_dir_and_file, temp_path_not_exist)
            with yield_zip(temp_path_not_exist) as temp:
                assert temp.is_dir()
                assert one(temp.iterdir()).is_file()

        def test_non_existent(self, tmp_path: Path, temp_path_not_exist: Path) -> None:
            temp_path_not_exist = tmp_path / "zip"
            compress_zip(temp_path_not_exist, temp_path_not_exist)
            with yield_zip(temp_path_not_exist) as temp:
                assert temp.is_dir()
                assert list(temp.iterdir()) == []
