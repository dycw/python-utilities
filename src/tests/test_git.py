from __future__ import annotations

from typing import TYPE_CHECKING

from hypothesis import given, settings
from hypothesis.strategies import DataObject, data
from pytest import raises

from utilities.git import (
    GetRepoRootError,
    fetch_all_tags,
    get_branch_name,
    get_ref_tags,
    get_repo_name,
    get_repo_root,
)
from utilities.hypothesis import git_repos, text_ascii

if TYPE_CHECKING:
    from pathlib import Path

    from hypothesis.strategies import DataObject


class TestFetchAllTags:
    @given(repo=git_repos())
    @settings(max_examples=1)
    def test_main(self, *, repo: Path) -> None:
        fetch_all_tags(cwd=repo)


class TestGetBranchName:
    @given(data=data(), branch=text_ascii(min_size=1))
    @settings(max_examples=1)
    def test_main(self, *, data: DataObject, branch: str) -> None:
        repo = data.draw(git_repos(branch=branch))
        result = get_branch_name(cwd=repo)
        assert result == branch


class TestGetRefTags:
    @given(repo=git_repos())
    @settings(max_examples=1)
    def test_main(self, *, repo: Path) -> None:
        tags = get_ref_tags("HEAD", cwd=repo)
        assert len(tags) == 0


class TestGetRepoName:
    @given(data=data(), name=text_ascii(min_size=1))
    @settings(max_examples=1)
    def test_main(self, *, data: DataObject, name: str) -> None:
        remote = f"https://localhost/{name}.git"
        repo = data.draw(git_repos(remote=remote))
        result = get_repo_name(cwd=repo)
        assert result == name


class TestGetRepoRoot:
    @given(repo=git_repos())
    @settings(max_examples=1)
    def test_main(self, *, repo: Path) -> None:
        root = get_repo_root(cwd=repo)
        expected = repo.resolve()
        assert root == expected

    def test_error(self, *, tmp_path: Path) -> None:
        with raises(
            GetRepoRootError, match="Path is not part of a `git` repository: .*"
        ):
            _ = get_repo_root(cwd=tmp_path)
