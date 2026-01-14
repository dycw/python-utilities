from __future__ import annotations

from pathlib import Path
from string import ascii_letters
from typing import TYPE_CHECKING
from zipfile import ZipFile

from hypothesis import given
from hypothesis.strategies import sampled_from, sets
from pytest import mark

from utilities.hypothesis import temp_paths, text_ascii, text_ascii_lower
from utilities.platform import maybe_lower_case
from utilities.text import unique_str
from utilities.zipfile import yield_zip_file_contents, zip_paths

if TYPE_CHECKING:
    from collections.abc import Set as AbstractSet


@mark.skip
class TestYieldZipFileContents:
    @mark.only
    def test_single_file(self, tmp_path: Path) -> None:
        src = tmp_path / "src"
        src.mkdir()
        filename = maybe_lower_case(unique_str())
        (src / filename).touch()
        with ZipFile(tmp_path, mode="w") as zf:
            zf.writestr(filename, filename)
        assert path_zip.exists()
        with yield_zip_file_contents(path_zip) as paths:
            assert isinstance(paths, list)
            assert len(paths) == len(filename)
            for path in paths:
                assert isinstance(path, Path)

    @given(
        temp_path=temp_paths(),
        contents=sets(sampled_from(ascii_letters), min_size=1, max_size=10),
    )
    def test_main(self, temp_path: Path, contents: AbstractSet[str]) -> None:
        contents = set(maybe_lower_case(contents))
        assert temp_path.exists()
        assert not list(temp_path.iterdir())
        path_zip = Path(temp_path, "zipfile")
        with ZipFile(path_zip, mode="w") as zf:
            for con in contents:
                zf.writestr(con, con)
        assert path_zip.exists()
        with yield_zip_file_contents(path_zip) as paths:
            assert isinstance(paths, list)
            assert len(paths) == len(contents)
            for path in paths:
                assert isinstance(path, Path)


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
            assert zf.namelist() == [p.name for p in temp_files]

    def test_dir_empty(self, tmp_path: Path) -> None:
        src = tmp_path / "src"
        src.mkdir()
        dest = tmp_path / "zip"
        zip_paths(src, dest)
        with ZipFile(dest) as zf:
            assert zf.namelist() == []

    def test_dir_single_file(self, tmp_path: Path) -> None:
        src = tmp_path / "src"
        src.mkdir()
        (src / "file.txt").touch()
        dest = tmp_path / "zip"
        zip_paths(src, dest)
        with ZipFile(dest) as zf:
            assert zf.namelist() == ["file.txt"]

    def test_dir_multiple_files(self, tmp_path: Path) -> None:
        src = tmp_path / "src"
        src.mkdir()
        (src / "file1.txt").touch()
        (src / "file2.txt").touch()
        dest = tmp_path / "zip"
        zip_paths(src, dest)
        with ZipFile(dest) as zf:
            assert zf.namelist() == ["file1.txt", "file2.txt"]

    def test_dir_nested(self, tmp_path: Path) -> None:
        src = tmp_path / "src"
        src.mkdir()
        (src / "file1.txt").touch()
        sub_dir = src / "sub_dir"
        sub_dir.mkdir()
        (sub_dir / "file2.txt").touch()
        dest = tmp_path / "zip"
        zip_paths(src, dest)
        with ZipFile(dest) as zf:
            assert zf.namelist() == ["file1.txt", "sub_dir/", "sub_dir/file2.txt"]
