from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from shutil import copytree
from typing import TYPE_CHECKING, Self

from hypothesis import HealthCheck, given, settings
from hypothesis.strategies import DataObject, data, integers, sets
from pytest import mark, param, raises

from tests.conftest import SKIPIF_CI_AND_WINDOWS
from utilities.dataclasses import replace_non_sentinel
from utilities.hypothesis import git_repos, paths, temp_paths
from utilities.pathlib import (
    GetRootError,
    ensure_suffix,
    expand_path,
    get_path,
    get_root,
    is_sub_path,
    list_dir,
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


class TestGetRoot:
    @given(data=data(), repo=git_repos())
    @settings(
        max_examples=1, suppress_health_check={HealthCheck.function_scoped_fixture}
    )
    def test_git_dir(self, *, data: DataObject, repo: Path) -> None:
        path = repo.joinpath(data.draw(paths()))
        path.mkdir(parents=True, exist_ok=True)
        root = get_root(path=path)
        expected = repo.resolve()
        assert root == expected

    @given(data=data(), repo=git_repos())
    @settings(
        max_examples=1, suppress_health_check={HealthCheck.function_scoped_fixture}
    )
    def test_git_file(self, *, data: DataObject, repo: Path) -> None:
        path = repo.joinpath(data.draw(paths(min_depth=1)))
        path.touch()
        root = get_root(path=path)
        expected = repo.resolve()
        assert root == expected

    @given(data=data())
    @settings(
        max_examples=1, suppress_health_check={HealthCheck.function_scoped_fixture}
    )
    def test_envrc(self, *, data: DataObject, tmp_path: Path) -> None:
        tmp_path.joinpath(".envrc").touch()
        path = tmp_path.joinpath(data.draw(paths()))
        result = get_root(path=path)
        expected = tmp_path.resolve()
        assert result == expected

    @given(data=data(), repo=git_repos())
    @settings(
        max_examples=1, suppress_health_check={HealthCheck.function_scoped_fixture}
    )
    def test_git_and_envrc(self, *, data: DataObject, repo: Path) -> None:
        repo.joinpath(".envrc").touch()
        path = repo.joinpath(data.draw(paths()))
        root = get_root(path=path)
        expected = repo.resolve()
        assert root == expected

    @given(data=data(), repo=git_repos())
    @settings(
        max_examples=1, suppress_health_check={HealthCheck.function_scoped_fixture}
    )
    def test_git_with_envrc_inside(self, *, data: DataObject, repo: Path) -> None:
        path = repo.joinpath(data.draw(paths(min_depth=1)))
        path.mkdir(parents=True)
        path.joinpath(".envrc").touch()
        root = get_root(path=path)
        expected = path.resolve()
        assert root == expected

    @given(data=data(), repo=git_repos())
    @settings(
        max_examples=1, suppress_health_check={HealthCheck.function_scoped_fixture}
    )
    def test_envrc_with_git_inside(self, *, data: DataObject, repo: Path) -> None:
        with TemporaryDirectory() as temp:
            temp.joinpath(".envrc").touch()
            path = temp.joinpath(data.draw(paths(min_depth=1)))
            _ = copytree(repo, path)
            root = get_root(path=path)
            expected = path.resolve()
            assert root == expected

    def test_error(self, *, tmp_path: Path) -> None:
        with raises(GetRootError, match="Unable to determine root from '.*'"):
            _ = get_root(path=tmp_path)


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


class TestTempCWD:
    def test_main(self, *, tmp_path: Path) -> None:
        assert Path.cwd() != tmp_path
        with temp_cwd(tmp_path):
            assert Path.cwd() == tmp_path
        assert Path.cwd() != tmp_path
