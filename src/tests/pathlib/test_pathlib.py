from __future__ import annotations

from pathlib import Path

from hypothesis import given
from hypothesis.strategies import booleans
from pytest import mark, param

from utilities.hypothesis import temp_paths
from utilities.pathlib import ensure_suffix, temp_cwd, walk


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
        assert result == Path(expected)


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
        assert Path.cwd() != tmp_path
        with temp_cwd(tmp_path):
            assert Path.cwd() == tmp_path
        assert Path.cwd() != tmp_path
