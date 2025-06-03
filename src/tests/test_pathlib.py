from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Self

from hypothesis import given, settings
from hypothesis.strategies import integers, sets
from pytest import mark, param, raises

from utilities.dataclasses import replace_non_sentinel
from utilities.hypothesis import git_repos, paths, temp_paths
from utilities.pathlib import (
    GetRootError,
    ensure_suffix,
    get_path,
    get_root,
    list_dir,
    temp_cwd,
)
from utilities.sentinel import Sentinel, sentinel

if TYPE_CHECKING:
    from utilities.types import MaybeCallablePathLike


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

    @given(path=paths())
    def test_str(self, *, path: Path) -> None:
        assert get_path(path=str(path)) == path

    def test_none(self) -> None:
        assert get_path(path=None) == Path.cwd()

    def test_sentinel(self) -> None:
        assert get_path(path=sentinel) is sentinel

    @given(path1=paths(), path2=paths())
    def test_replace_non_sentinel(self, *, path1: Path, path2: Path) -> None:
        @dataclass(kw_only=True, slots=True)
        class Example:
            path: Path = field(default_factory=Path.cwd)

            def replace(
                self, *, path: MaybeCallablePathLike | Sentinel = sentinel
            ) -> Self:
                return replace_non_sentinel(self, path=get_path(path=path))

        obj = Example(path=path1)
        assert obj.path == path1
        assert obj.replace().path == path1
        assert obj.replace(path=path2).path == path2

    @given(path=paths())
    def test_callable(self, *, path: Path) -> None:
        assert get_path(path=lambda: path) == path


class TestGetRoot:
    @given(repo=git_repos())
    @settings(max_examples=1)
    def test_git(self, *, repo: Path) -> None:
        root = get_root(path=repo)
        expected = repo.resolve()
        assert root == expected

    @given(root=temp_paths())
    @settings(max_examples=1)
    def test_envrc(self, *, root: Path) -> None:
        root.joinpath(".envrc").touch()
        result = get_root(path=root)
        assert result == root

    @given(root=temp_paths())
    @settings(max_examples=1)
    def test_envrc_from_inside(self, *, root: Path) -> None:
        root.joinpath(".envrc").touch()
        path = root.joinpath("foo", "bar", "baz")
        path.mkdir(parents=True)
        result = get_root(path=path)
        assert result == root

    def test_error(self, *, tmp_path: Path) -> None:
        with raises(GetRootError, match="Unable to determine root from '.*'"):
            _ = get_root(path=tmp_path)


class TestListDir:
    @given(root=temp_paths(), nums=sets(integers(0, 100), max_size=10))
    def test_main(self, *, root: Path, nums: set[str]) -> None:
        for n in nums:
            path = root.joinpath(f"{n}.txt")
            path.touch()
        result = list_dir(root)
        expected = sorted(Path(root, f"{n}.txt") for n in nums)
        assert result == expected


class TestTempCWD:
    def test_main(self, *, tmp_path: Path) -> None:
        assert Path.cwd() != tmp_path
        with temp_cwd(tmp_path):
            assert Path.cwd() == tmp_path
        assert Path.cwd() != tmp_path
