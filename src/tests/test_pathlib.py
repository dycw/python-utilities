from __future__ import annotations

import datetime as dt
from pathlib import Path

from hypothesis import given
from hypothesis.strategies import booleans, integers, sets
from pathvalidate import ValidationError
from pytest import mark, param, raises

from utilities.hypothesis import temp_paths
from utilities.pathlib import (
    ensure_path,
    ensure_suffix,
    get_modified_time,
    list_dir,
    temp_cwd,
    walk,
)
from utilities.pathvalidate import valid_path, valid_path_cwd


class TestEnsurePath:
    def test_main(self) -> None:
        assert isinstance(ensure_path(Path("abc")), Path)

    def test_error_validation(self) -> None:
        with raises(ValidationError):
            _ = ensure_path("\0", validate=True)

    def test_error_sanitized(self) -> None:
        assert ensure_path("a\0b", sanitize=True) == Path("ab")


class TestEnsureSuffix:
    @mark.parametrize(
        ("path", "expected"),
        [
            param("hello.txt", "hello.txt"),
            param("hello.1.txt", "hello.1.txt"),
            param("hello.1.2.txt", "hello.1.2.txt"),
            param("hello.jpg", "hello.jpg.txt"),
            param("hello.1.jpg", "hello.1.jpg.txt"),
            param("hello.1.2.jpg", "hello.1.2.jpg.txt"),
            param("hello.txt.jpg", "hello.txt.jpg.txt"),
            param("hello.txt.1.jpg", "hello.txt.1.jpg.txt"),
            param("hello.txt.1.2.jpg", "hello.txt.1.2.jpg.txt"),
        ],
    )
    def test_main(self, *, path: Path, expected: Path) -> None:
        result = ensure_suffix(path, ".txt")
        assert result == valid_path(expected)


class TestGetModifiedTime:
    @given(path=temp_paths())
    def test_main(self, *, path: Path) -> None:
        path.touch()
        mod = get_modified_time(path)
        assert isinstance(mod, dt.datetime)


class TestListDir:
    @given(root=temp_paths(), nums=sets(integers(0, 100), max_size=10))
    def test_main(self, *, root: Path, nums: set[str]) -> None:
        for n in nums:
            path = root.joinpath(f"{n}.txt")
            path.touch()
        result = list_dir(root)
        expected = sorted(Path(root, f"{n}.txt") for n in nums)
        assert result == expected


class TestWalk:
    @given(
        root=temp_paths(),
        topdown=booleans(),
        onerror=booleans(),
        followlinks=booleans(),
    )
    def test_main(
        self, *, root: Path, topdown: bool, onerror: bool, followlinks: bool
    ) -> None:
        def on_error(error: OSError, /) -> None:
            assert error.args != ()

        for dirpath, dirnames, filenames in walk(
            root,
            topdown=topdown,
            onerror=on_error if onerror else None,
            followlinks=followlinks,
        ):
            assert isinstance(dirpath, Path)
            assert isinstance(dirnames, list)
            for dirname in dirnames:
                assert isinstance(dirname, Path)
                assert dirname.is_dir()
            for filename in filenames:
                assert isinstance(filename, Path)
                assert filename.is_file()


class TestTempCWD:
    def test_main(self, *, tmp_path: Path) -> None:
        assert valid_path_cwd() != tmp_path
        with temp_cwd(tmp_path):
            assert valid_path_cwd() == tmp_path
        assert valid_path_cwd() != tmp_path
