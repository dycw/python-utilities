import datetime as dt
from pathlib import Path

import pytest
from hypothesis import given
from hypothesis.strategies import booleans
from pathvalidate import ValidationError

from utilities.hypothesis import temp_paths
from utilities.pathlib import (
    ensure_path,
    ensure_suffix,
    get_modified_time,
    temp_cwd,
    walk,
)
from utilities.pathvalidate import valid_path, valid_path_cwd


class TestEnsurePath:
    def test_main(self) -> None:
        assert isinstance(ensure_path(Path("abc")), Path)

    def test_error_validation(self) -> None:
        with pytest.raises(ValidationError):
            _ = ensure_path("\0", validate=True)

    def test_error_sanitized(self) -> None:
        assert ensure_path("a\0b", sanitize=True) == Path("ab")


class TestEnsureSuffix:
    @pytest.mark.parametrize(
        ("path", "expected"),
        [
            pytest.param("hello.txt", "hello.txt"),
            pytest.param("hello.1.txt", "hello.1.txt"),
            pytest.param("hello.1.2.txt", "hello.1.2.txt"),
            pytest.param("hello.jpg", "hello.jpg.txt"),
            pytest.param("hello.1.jpg", "hello.1.jpg.txt"),
            pytest.param("hello.1.2.jpg", "hello.1.2.jpg.txt"),
            pytest.param("hello.txt.jpg", "hello.txt.jpg.txt"),
            pytest.param("hello.txt.1.jpg", "hello.txt.1.jpg.txt"),
            pytest.param("hello.txt.1.2.jpg", "hello.txt.1.2.jpg.txt"),
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
