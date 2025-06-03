from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Self

from hypothesis import given
from hypothesis.strategies import integers, none, sets
from pytest import mark, param

from utilities.dataclasses import replace_non_sentinel
from utilities.hypothesis import paths, sentinels, temp_paths
from utilities.pathlib import ensure_suffix, get_path, list_dir, resolve_path, temp_cwd
from utilities.sentinel import Sentinel, sentinel

if TYPE_CHECKING:
    from utilities.types import MaybeCallablePath


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


class TestGetPath:
    @given(path=paths())
    def test_path(self, *, path: Path) -> None:
        assert get_path(path=path) == path

    @given(path=none() | sentinels())
    def test_none_or_sentinel(self, *, path: None | Sentinel) -> None:
        assert get_path(path=path) is path

    @given(path1=paths(), path2=paths())
    def test_replace_non_sentinel(self, *, path1: Path, path2: Path) -> None:
        @dataclass(kw_only=True, slots=True)
        class Example:
            path: Path = field(default_factory=get_path)

            def replace(self, *, path: MaybeCallablePath | Sentinel = sentinel) -> Self:
                return replace_non_sentinel(self, path=get_path(path=path))

        obj = Example(path=path1)
        assert obj.path == path1
        assert obj.replace().path == path1
        assert obj.replace(path=path2).path == path2

    @given(path=paths())
    def test_callable(self, *, path: Path) -> None:
        assert get_path(path=lambda: path) == path


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
