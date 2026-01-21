from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Self, assert_never

from hypothesis import given, settings
from hypothesis.strategies import integers, sets
from pytest import mark, param, raises

from utilities.constants import SYSTEM, Sentinel, sentinel
from utilities.dataclasses import replace_non_sentinel
from utilities.hypothesis import git_repos, pairs, paths, temp_paths
from utilities.pathlib import (
    _GetRepoRootNotARepoError,
    _GetTailDisambiguate,
    _GetTailEmptyError,
    _GetTailLengthError,
    _GetTailNonUniqueError,
    ensure_suffix,
    get_file_group,
    get_file_owner,
    get_repo_root,
    get_tail,
    list_dir,
    module_path,
    to_path,
)

if TYPE_CHECKING:
    from utilities.types import MaybeCallablePathLike, PathLike


class TestEnsureSuffix:
    @mark.parametrize(
        ("path", "suffix", "expected"),
        [
            param("foo", ".txt", "foo.txt"),
            param("foo.txt", ".txt", "foo.txt"),
            param("foo.bar.baz", ".baz", "foo.bar.baz"),
            param("foo.bar.baz", ".quux", "foo.bar.baz.quux"),
            param("foo", ".txt.gz", "foo.txt.gz"),
            param("foo.txt", ".txt.gz", "foo.txt.gz"),
            param("foo.bar.baz", ".bar.baz", "foo.bar.baz"),
        ],
        ids=str,
    )
    def test_main(self, *, path: Path, suffix: str, expected: str) -> None:
        result = str(ensure_suffix(path, suffix))
        assert result == expected


class TestFileOwnerAndGroup:
    def test_owner(self, *, tmp_path: Path) -> None:
        path = tmp_path.joinpath("file.txt")
        path.touch()
        self._assert(get_file_owner(path))

    def test_group(self, *, tmp_path: Path) -> None:
        path = tmp_path.joinpath("file.txt")
        path.touch()
        self._assert(get_file_group(path))

    def _assert(self, value: str | None, /) -> None:
        match SYSTEM:
            case "windows":  # skipif-not-windows
                assert value is None
            case "mac" | "linux":  # skipif-windows
                assert isinstance(value, str)
            case never:
                assert_never(never)


class TestGetRepoRoot:
    @given(repo=git_repos(), tail=paths())
    @settings(max_examples=1)
    def test_dir(self, *, repo: Path, tail: Path) -> None:
        root = get_repo_root(repo.joinpath(tail))
        expected = repo.resolve()
        assert root == expected

    @given(repo=git_repos(), tail=paths(min_depth=1))
    @settings(max_examples=1)
    def test_file(self, *, repo: Path, tail: Path) -> None:
        path = repo.joinpath(tail)
        path.touch()
        root = get_repo_root(path)
        expected = repo.resolve()
        assert root == expected

    def test_error_not_a_repo(self, *, tmp_path: Path) -> None:
        with raises(
            _GetRepoRootNotARepoError,
            match=r"Path is not part of a `git` repository: .*",
        ):
            _ = get_repo_root(tmp_path)


class TestGetTail:
    @mark.parametrize(
        ("path", "root", "disambiguate", "expected"),
        [
            param("foo/bar/baz", "foo", "raise", Path("bar/baz")),
            param("foo/bar/baz", "foo/bar", "raise", Path("baz")),
            param("a/b/c/d/a/b/c/d/e", "b/c", "earlier", Path("d/a/b/c/d/e")),
            param("a/b/c/d/a/b/c/d/e", "b/c", "later", Path("d/e")),
        ],
    )
    def test_main(
        self,
        *,
        path: PathLike,
        root: PathLike,
        disambiguate: _GetTailDisambiguate,
        expected: Path,
    ) -> None:
        tail = get_tail(path, root, disambiguate=disambiguate)
        assert tail == expected

    def test_error_length(self) -> None:
        with raises(
            _GetTailLengthError,
            match=r"Unable to get the tail of 'foo' with root of length 2",
        ):
            _ = get_tail("foo", "bar/baz")

    def test_error_empty(self) -> None:
        with raises(
            _GetTailEmptyError,
            match=r"Unable to get the tail of 'foo/bar' with root 'baz'",
        ):
            _ = get_tail("foo/bar", "baz")

    def test_error_non_unique(self) -> None:
        with raises(
            _GetTailNonUniqueError,
            match=r"Path '.*' must contain exactly one tail with root 'b'; got '.*', '.*' and perhaps more",
        ):
            _ = get_tail("a/b/c/a/b/c", "b")


class TestListDir:
    @given(root=temp_paths(), nums=sets(integers(0, 100), max_size=10))
    def test_main(self, *, root: Path, nums: set[str]) -> None:
        for n in nums:
            path = root.joinpath(f"{n}.txt")
            path.touch()
        result = list_dir(root)
        expected = sorted(Path(root, f"{n}.txt") for n in nums)
        assert result == expected


class TestModulePath:
    @mark.parametrize(
        ("root", "expected"),
        [param(None, "foo.bar.baz"), param("foo", "bar.baz"), param("foo/bar", "baz")],
    )
    def test_main(self, *, root: PathLike | None, expected: Path) -> None:
        module = module_path("foo/bar/baz.py", root=root)
        assert module == expected


class TestToPath:
    def test_default(self) -> None:
        assert to_path() == Path.cwd()

    @given(path=paths())
    def test_path(self, *, path: Path) -> None:
        assert to_path(path) == path

    @given(path=paths())
    def test_str(self, *, path: Path) -> None:
        assert to_path(str(path)) == path

    @given(path=paths())
    def test_callable(self, *, path: Path) -> None:
        assert to_path(lambda: path) == path

    def test_none(self) -> None:
        assert to_path(None) == Path.cwd()

    def test_sentinel(self) -> None:
        assert to_path(sentinel) is sentinel

    @given(paths=pairs(paths()))
    def test_replace_non_sentinel(self, *, paths: tuple[Path, Path]) -> None:
        path1, path2 = paths

        @dataclass(kw_only=True, slots=True)
        class Example:
            path: Path = field(default_factory=Path.cwd)

            def replace(
                self, *, path: MaybeCallablePathLike | Sentinel = sentinel
            ) -> Self:
                return replace_non_sentinel(self, path=to_path(path))

        obj = Example(path=path1)
        assert obj.path == path1
        assert obj.replace().path == path1
        assert obj.replace(path=path2).path == path2
