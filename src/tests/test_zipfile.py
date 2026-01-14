from __future__ import annotations

from typing import TYPE_CHECKING
from zipfile import ZipFile

from utilities.iterables import one
from utilities.zipfile import yield_zip_file_contents, zip_paths

if TYPE_CHECKING:
    from pathlib import Path


class TestYieldZipFileContents:
    def test_single_file(self, tmp_path: Path, temp_file: Path) -> None:
        dest = tmp_path / "zip"
        zip_paths(temp_file, dest)
        with yield_zip_file_contents(dest) as temp:
            assert temp.is_file()
            assert temp.name == temp_file.name

    def test_multiple_files(
        self, tmp_path: Path, temp_files: tuple[Path, Path]
    ) -> None:
        path1, path2 = temp_files
        dest = tmp_path / "zip"
        zip_paths(path1, path2, dest)
        with yield_zip_file_contents(dest) as temp:
            assert temp.is_dir()
            result = {p.name for p in temp.iterdir()}
        expected = {p.name for p in temp_files}
        assert result == expected

    def test_dir_single_file(self, tmp_path: Path) -> None:
        src = tmp_path / "src"
        src.mkdir()
        (src / "file.txt").touch()
        dest = tmp_path / "zip"
        zip_paths(src, dest)
        with yield_zip_file_contents(dest) as temp:
            assert temp.is_file()
            assert temp.name == "file.txt"


class TestZipPath:
    def test_single_file(self, tmp_path: Path, temp_file: Path) -> None:
        dest = tmp_path / "zip"
        zip_paths(temp_file, dest)
        with ZipFile(dest) as zf:
            assert zf.namelist() == [temp_file.name]

    def test_multiple_files(
        self, tmp_path: Path, temp_files: tuple[Path, Path]
    ) -> None:
        path1, path2 = temp_files
        dest = tmp_path / "zip"
        zip_paths(path1, path2, dest)
        with ZipFile(dest) as zf:
            assert set(zf.namelist()) == {p.name for p in temp_files}

    def test_dir_empty(self, tmp_path: Path, temp_dir_with_nothing: Path) -> None:
        dest = tmp_path / "zip"
        zip_paths(temp_dir_with_nothing, dest)
        with ZipFile(dest) as zf:
            assert zf.namelist() == []

    def test_dir_single_file(self, tmp_path: Path, temp_dir_with_file: Path) -> None:
        dest = tmp_path / "zip"
        zip_paths(temp_dir_with_file, dest)
        with ZipFile(dest) as zf:
            result = zf.namelist()
        expected = [one(temp_dir_with_file.iterdir()).name]
        assert result == expected

    def test_dir_multiple_files(
        self, tmp_path: Path, temp_dir_with_files: Path
    ) -> None:
        dest = tmp_path / "zip"
        zip_paths(temp_dir_with_files, dest)
        with ZipFile(dest) as zf:
            result = set(zf.namelist())
        expected = {p.name for p in temp_dir_with_files.iterdir()}
        assert result == expected

    def test_dir_nested(self, tmp_path: Path, temp_dir_with_dir_and_file: Path) -> None:
        dest = tmp_path / "zip"
        zip_paths(temp_dir_with_dir_and_file, dest)
        with ZipFile(dest) as zf:
            result = list(zf.namelist())
        inner = one(temp_dir_with_dir_and_file.iterdir())
        expected = [f"{inner.name}/", f"{inner.name}/{one(inner.iterdir()).name}"]
        assert result == expected

    def test_non_existent(self, tmp_path: Path, temp_path_not_exist: Path) -> None:
        dest = tmp_path / "zip"
        zip_paths(temp_path_not_exist, dest)
        with ZipFile(dest) as zf:
            assert zf.namelist() == []
