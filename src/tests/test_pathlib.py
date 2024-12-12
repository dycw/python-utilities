from __future__ import annotations

from pathlib import Path

from hypothesis import given
from hypothesis.strategies import integers, sets
from pytest import mark, param

from utilities.hypothesis import temp_paths
from utilities.pathlib import ensure_suffix, list_dir, resolve_path, temp_cwd


class TestEnsureSuffix:
    @mark.parametrize(
        ("path", "suffix", "expected"),
        [
            param("foo", ".txt", "foo.txt"),
            param("foo.txt", ".txt", "foo.txt"),
            param("foo.bar.baz", ".baz", "foo.bar.baz"),
            param("foo.bar.baz", ".quux", "foo.bar.baz.quux"),
        ],
        ids=str,
    )
    def test_main(self, *, path: Path, suffix: str, expected: str) -> None:
        result = str(ensure_suffix(path, suffix))
        assert result == expected


class TestListDir:
    @given(root=temp_paths(), nums=sets(integers(0, 100), max_size=10))
    def test_main(self, *, root: Path, nums: set[str]) -> None:
        for n in nums:
            path = root.joinpath(f"{n}.txt")
            path.touch()
        result = list_dir(root)
        expected = sorted(Path(root, f"{n}.txt") for n in nums)
        assert result == expected


class TestResolvePath:
    def test_cwd(self, *, tmp_path: Path) -> None:
        with temp_cwd(tmp_path):
            result = resolve_path()
        assert result == tmp_path

    def test_path(self, *, tmp_path: Path) -> None:
        result = resolve_path(path=tmp_path)
        assert result == tmp_path

    def test_callable(self, *, tmp_path: Path) -> None:
        result = resolve_path(path=lambda: tmp_path)
        assert result == tmp_path


class TestTempCWD:
    def test_main(self, *, tmp_path: Path) -> None:
        assert Path.cwd() != tmp_path
        with temp_cwd(tmp_path):
            assert Path.cwd() == tmp_path
        assert Path.cwd() != tmp_path
