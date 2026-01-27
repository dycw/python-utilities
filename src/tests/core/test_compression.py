from __future__ import annotations

import re
from re import DOTALL
from typing import TYPE_CHECKING

from pytest import fixture, raises

from utilities.core import (
    CompressBZ2Error,
    CompressGzipError,
    CompressLZMAError,
    CompressZipError,
    YieldBZ2FileNotFoundError,
    YieldBZ2IsADirectoryError,
    YieldBZ2NotADirectoryError,
    YieldGzipFileNotFoundError,
    YieldGzipIsADirectoryError,
    YieldGzipNotADirectoryError,
    YieldLZMAFileNotFoundError,
    YieldLZMAIsADirectoryError,
    YieldLZMANotADirectoryError,
    YieldZipFileNotFoundError,
    YieldZipIsADirectoryError,
    YieldZipNotADirectoryError,
    compress_bz2,
    compress_gzip,
    compress_lzma,
    compress_zip,
    one,
    yield_bz2,
    yield_gzip,
    yield_lzma,
    yield_zip,
)

if TYPE_CHECKING:
    from collections.abc import Callable
    from contextlib import AbstractContextManager
    from pathlib import Path

    from _pytest.fixtures import SubRequest

    from utilities.types import PathLike


type Compress = Callable[..., None]
type YieldUncompressed = Callable[[PathLike], AbstractContextManager[Path]]
type Data = tuple[
    Compress,
    YieldUncompressed,
    type[Exception],
    type[Exception],
    type[Exception],
    type[Exception],
]


@fixture(
    params=[
        (
            compress_bz2,
            yield_bz2,
            CompressBZ2Error,
            YieldBZ2FileNotFoundError,
            YieldBZ2IsADirectoryError,
            YieldBZ2NotADirectoryError,
        ),
        (
            compress_gzip,
            yield_gzip,
            CompressGzipError,
            YieldGzipFileNotFoundError,
            YieldGzipIsADirectoryError,
            YieldGzipNotADirectoryError,
        ),
        (
            compress_lzma,
            yield_lzma,
            CompressLZMAError,
            YieldLZMAFileNotFoundError,
            YieldLZMAIsADirectoryError,
            YieldLZMANotADirectoryError,
        ),
        (
            compress_zip,
            yield_zip,
            CompressZipError,
            YieldZipFileNotFoundError,
            YieldZipIsADirectoryError,
            YieldZipNotADirectoryError,
        ),
    ]
)
def data(*, request: SubRequest) -> Data:
    return request.param


@fixture
def compress(*, data: Data) -> Compress:
    compress, _, _, _, _, _ = data
    return compress


@fixture
def yield_uncompressed(*, data: Data) -> YieldUncompressed:
    _, yield_uncompressed, _, _, _, _ = data
    return yield_uncompressed


@fixture
def error_compress(*, data: Data) -> type[Exception]:
    _, _, error, _, _, _ = data
    return error


@fixture
def error_yield_uncompressed_file_not_found(*, data: Data) -> type[Exception]:
    _, _, _, error, _, _ = data
    return error


@fixture
def error_yield_uncompressed_is_a_directory(*, data: Data) -> type[Exception]:
    _, _, _, _, error, _ = data
    return error


@fixture
def error_yield_uncompressed_not_a_directory(*, data: Data) -> type[Exception]:
    _, _, _, _, _, error = data
    return error


# all


class TestCompressAndYieldUncompressed:
    def test_single_file(
        self,
        *,
        temp_file: Path,
        temp_path_not_exist: Path,
        compress: Compress,
        yield_uncompressed: YieldUncompressed,
    ) -> None:
        _ = temp_file.write_text("text")
        compress(temp_file, temp_path_not_exist)
        with yield_uncompressed(temp_path_not_exist) as temp:
            assert temp.is_file()
            assert temp.read_text() == "text"

    def test_multiple_files(
        self,
        *,
        temp_files: tuple[Path, Path],
        temp_path_not_exist: Path,
        compress: Compress,
        yield_uncompressed: YieldUncompressed,
    ) -> None:
        path1, path2 = temp_files
        _ = path1.write_text("text1")
        _ = path2.write_text("text2")
        compress(path1, path2, temp_path_not_exist)
        with yield_uncompressed(temp_path_not_exist) as temp:
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
        compress: Compress,
        yield_uncompressed: YieldUncompressed,
    ) -> None:
        compress(temp_dir_with_nothing, temp_path_not_exist)
        with yield_uncompressed(temp_path_not_exist) as temp:
            assert temp.is_dir()
            assert len(list(temp.iterdir())) == 0

    def test_single_dir_single_file(
        self,
        *,
        temp_dir_with_file: Path,
        temp_path_not_exist: Path,
        compress: Compress,
        yield_uncompressed: YieldUncompressed,
    ) -> None:
        inner = one(temp_dir_with_file.iterdir())
        _ = inner.write_text("text")
        compress(temp_dir_with_file, temp_path_not_exist)
        with yield_uncompressed(temp_path_not_exist) as temp:
            assert temp.is_file()
            assert temp.read_text() == "text"

    def test_single_dir_multiple_files(
        self,
        *,
        temp_dir_with_files: Path,
        temp_path_not_exist: Path,
        compress: Compress,
        yield_uncompressed: YieldUncompressed,
    ) -> None:
        path1, path2 = sorted(temp_dir_with_files.iterdir())
        _ = path1.write_text("text1")
        _ = path2.write_text("text2")
        compress(temp_dir_with_files, temp_path_not_exist)
        with yield_uncompressed(temp_path_not_exist) as temp:
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
        compress: Compress,
        yield_uncompressed: YieldUncompressed,
    ) -> None:
        inner_dir = one(temp_dir_with_dir_and_file.iterdir())
        inner_file = one(inner_dir.iterdir())
        _ = inner_file.write_text("text")
        compress(temp_dir_with_dir_and_file, temp_path_not_exist)
        with yield_uncompressed(temp_path_not_exist) as temp:
            assert temp.is_dir()
            assert len(list(temp.iterdir())) == 1
            assert (temp / inner_file.name).is_file()
            assert (temp / inner_file.name).read_text() == "text"

    def test_multiple_dirs(
        self,
        *,
        temp_dirs_with_files: tuple[Path, Path],
        temp_path_not_exist: Path,
        compress: Compress,
        yield_uncompressed: YieldUncompressed,
    ) -> None:
        dir1, dir2 = temp_dirs_with_files
        file1 = one(dir1.iterdir())
        _ = file1.write_text("text1")
        file2 = one(dir2.iterdir())
        _ = file2.write_text("text2")
        compress(dir1, dir2, temp_path_not_exist)
        with yield_uncompressed(temp_path_not_exist) as temp:
            assert temp.is_dir()
            assert len(list(temp.iterdir())) == 2
            assert (temp / file1.name).is_file()
            assert (temp / file1.name).read_text() == "text1"
            assert (temp / file2.name).is_file()
            assert (temp / file2.name).read_text() == "text2"

    def test_single_non_existent(
        self,
        *,
        temp_path_not_exist: Path,
        compress: Compress,
        yield_uncompressed: YieldUncompressed,
    ) -> None:
        compress(temp_path_not_exist, temp_path_not_exist)
        with yield_uncompressed(temp_path_not_exist) as temp:
            assert temp.is_dir()
            assert len(list(temp.iterdir())) == 0

    def test_multiple_non_existent(
        self,
        *,
        temp_path_not_exist: Path,
        compress: Compress,
        yield_uncompressed: YieldUncompressed,
    ) -> None:
        compress(temp_path_not_exist, temp_path_not_exist, temp_path_not_exist)
        with yield_uncompressed(temp_path_not_exist) as temp:
            assert temp.is_dir()
            assert len(list(temp.iterdir())) == 0

    def test_error_compress(
        self,
        *,
        temp_files: tuple[Path, Path],
        compress: Compress,
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
            compress(src, dest)

    def test_error_yield_uncompressed_file_not_found(
        self,
        *,
        temp_path_not_exist: Path,
        yield_uncompressed: YieldUncompressed,
        error_yield_uncompressed_file_not_found: type[Exception],
    ) -> None:
        with (
            raises(
                error_yield_uncompressed_file_not_found,
                match=r"Cannot uncompress '.*' since it does not exist",
            ),
            yield_uncompressed(temp_path_not_exist),
        ):
            ...

    def test_error_yield_uncompressed_is_a_directory(
        self,
        *,
        tmp_path: Path,
        yield_uncompressed: YieldUncompressed,
        error_yield_uncompressed_is_a_directory: type[Exception],
    ) -> None:
        with (
            raises(
                error_yield_uncompressed_is_a_directory,
                match=r"Cannot uncompress '.*' since it is a directory",
            ),
            yield_uncompressed(tmp_path),
        ):
            ...

    def test_error_yield_uncompressed_not_a_directory(
        self,
        *,
        temp_path_parent_file: Path,
        yield_uncompressed: YieldUncompressed,
        error_yield_uncompressed_not_a_directory: type[Exception],
    ) -> None:
        with (
            raises(
                error_yield_uncompressed_not_a_directory,
                match=r"Cannot uncompress '.*' since its parent '.*' is not a directory",
            ),
            yield_uncompressed(temp_path_parent_file),
        ):
            ...
