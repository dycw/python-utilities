from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from shutil import copytree
from typing import TYPE_CHECKING, Self

from hypothesis import HealthCheck, given, settings
from hypothesis.strategies import integers, sets
from pytest import mark, param, raises

from tests.conftest import SKIPIF_CI_AND_WINDOWS
from utilities.dataclasses import replace_non_sentinel
from utilities.hypothesis import git_repos, paths, temp_paths
from utilities.pathlib import (
    GetPackageRootError,
    GetRepoRootError,
    GetRootError,
    _GetTailDisambiguate,
    _GetTailEmptyError,
    _GetTailLengthError,
    _GetTailNonUniqueError,
    ensure_suffix,
    expand_path,
    get_package_root,
    get_path,
    get_repo_root,
    get_root,
    get_tail,
    is_sub_path,
    list_dir,
    module_path,
    temp_cwd,
)
from utilities.sentinel import Sentinel, sentinel
from utilities.tempfile import TemporaryDirectory

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


class TestExpandPath:
    @mark.parametrize(
        ("path", "expected"),
        [
            param("foo", Path("foo")),
            param("~", Path.home()),
            param("~/foo", Path.home().joinpath("foo")),
            param("$HOME", Path.home(), marks=SKIPIF_CI_AND_WINDOWS),
            param(
                "$HOME/foo", Path.home().joinpath("foo"), marks=SKIPIF_CI_AND_WINDOWS
            ),
        ],
        ids=str,
    )
    def test_main(self, *, path: Path, expected: Path) -> None:
        result = expand_path(path)
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


class TestGetPackageRoot:
    @given(tail=paths())
    @settings(
        max_examples=1, suppress_health_check={HealthCheck.function_scoped_fixture}
    )
    def test_dir(self, *, tmp_path: Path, tail: Path) -> None:
        tmp_path.joinpath("pyproject.toml").touch()
        path = tmp_path.joinpath(tail)
        result = get_package_root(path=path)
        expected = tmp_path.resolve()
        assert result == expected

    @given(tail=paths(min_depth=1))
    @settings(
        max_examples=1, suppress_health_check={HealthCheck.function_scoped_fixture}
    )
    def test_file(self, *, tmp_path: Path, tail: Path) -> None:
        tmp_path.joinpath("pyproject.toml").touch()
        path = tmp_path.joinpath(tail)
        path.touch()
        root = get_package_root(path=path)
        expected = tmp_path.resolve()
        assert root == expected

    def test_error(self, *, tmp_path: Path) -> None:
        with raises(GetPackageRootError, match="Path is not part of a package: .*"):
            _ = get_package_root(path=tmp_path)


class TestGetRepoRoot:
    @given(repo=git_repos(), tail=paths())
    @settings(max_examples=1)
    def test_dir(self, *, repo: Path, tail: Path) -> None:
        root = get_repo_root(path=repo.joinpath(tail))
        expected = repo.resolve()
        assert root == expected

    @given(repo=git_repos(), tail=paths(min_depth=1))
    @settings(max_examples=1)
    def test_file(self, *, repo: Path, tail: Path) -> None:
        path = repo.joinpath(tail)
        path.touch()
        root = get_repo_root(path=path)
        expected = repo.resolve()
        assert root == expected

    def test_error(self, *, tmp_path: Path) -> None:
        with raises(
            GetRepoRootError, match="Path is not part of a `git` repository: .*"
        ):
            _ = get_repo_root(path=tmp_path)


class TestGetRoot:
    @given(repo=git_repos(), tail=paths())
    @settings(max_examples=1)
    def test_repo_only(self, *, repo: Path, tail: Path) -> None:
        root = get_root(path=repo.joinpath(tail))
        expected = repo.resolve()
        assert root == expected

    @given(tail=paths())
    @settings(
        max_examples=1, suppress_health_check={HealthCheck.function_scoped_fixture}
    )
    def test_package_only(self, *, tmp_path: Path, tail: Path) -> None:
        tmp_path.joinpath("pyproject.toml").touch()
        path = tmp_path.joinpath(tail)
        result = get_root(path=path)
        expected = tmp_path.resolve()
        assert result == expected

    @given(repo=git_repos(), tail=paths())
    @settings(
        max_examples=1, suppress_health_check={HealthCheck.function_scoped_fixture}
    )
    def test_repo_and_package(self, *, repo: Path, tail: Path) -> None:
        repo.joinpath("pyproject.toml").touch()
        path = repo.joinpath(tail)
        root = get_root(path=path)
        expected = repo.resolve()
        assert root == expected

    @given(repo=git_repos(), tail=paths(min_depth=1))
    @settings(
        max_examples=1, suppress_health_check={HealthCheck.function_scoped_fixture}
    )
    def test_repo_with_package_inside(self, *, repo: Path, tail: Path) -> None:
        path = repo.joinpath(tail)
        path.mkdir(parents=True)
        path.joinpath("pyproject.toml").touch()
        root = get_root(path=path)
        expected = path.resolve()
        assert root == expected

    @given(repo=git_repos(), tail=paths(min_depth=1))
    @settings(
        max_examples=1, suppress_health_check={HealthCheck.function_scoped_fixture}
    )
    def test_package_with_repo_inside(self, *, repo: Path, tail: Path) -> None:
        with TemporaryDirectory() as temp:
            temp.joinpath("pyproject.toml").touch()
            path = temp.joinpath(tail)
            _ = copytree(repo, path)
            root = get_root(path=path)
            expected = path.resolve()
            assert root == expected

    def test_error(self, *, tmp_path: Path) -> None:
        with raises(GetRootError, match="Unable to determine root from '.*'"):
            _ = get_root(path=tmp_path)


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
            match="Unable to get the tail of 'foo' with root of length 2",
        ):
            _ = get_tail("foo", "bar/baz")

    def test_error_empty(self) -> None:
        with raises(
            _GetTailEmptyError,
            match="Unable to get the tail of 'foo/bar' with root 'baz'",
        ):
            _ = get_tail("foo/bar", "baz")

    def test_error_non_unique(self) -> None:
        with raises(
            _GetTailNonUniqueError,
            match="Path '.*' must contain exactly one tail with root 'b'; got '.*', '.*' and perhaps more",
        ):
            _ = get_tail("a/b/c/a/b/c", "b")


class TestIsSubPath:
    @mark.parametrize(
        ("x", "y", "strict", "expected"),
        [
            param("foo", "foo", False, True),
            param("foo", "foo", True, False),
            param("foo/bar", "foo", False, True),
            param("foo/bar", "foo", True, True),
            param("foo/bar", "foo/baz", False, False),
            param("foo/bar", "foo/baz", True, False),
            param("foo", "foo/bar", False, False),
            param("foo", "foo/bar", True, False),
        ],
    )
    def test_main(
        self, *, x: PathLike, y: PathLike, strict: bool, expected: bool
    ) -> None:
        result = is_sub_path(x, y, strict=strict)
        assert result is expected


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


class TestTempCWD:
    def test_main(self, *, tmp_path: Path) -> None:
        assert Path.cwd() != tmp_path
        with temp_cwd(tmp_path):
            assert Path.cwd() == tmp_path
        assert Path.cwd() != tmp_path
