from __future__ import annotations

from typing import TYPE_CHECKING

from hypothesis import given
from hypothesis.strategies import DataObject, data
from pytest import raises

from utilities.git import (
    GetRepoRootError,
    get_branch_name,
    get_repo_name,
    get_repo_root,
)
from utilities.hypothesis import git_repos, settings_with_reduced_examples, text_ascii

if TYPE_CHECKING:
    from pathlib import Path


class TestGetBranchName:
    @given(data=data(), branch=text_ascii(min_size=1))
    @settings_with_reduced_examples()
    def test_main(self, *, data: DataObject, branch: str) -> None:
        root = data.draw(git_repos(branch=branch))
        result = get_branch_name(cwd=root)
        assert result == branch


class TestGetRepoName:
    def test_main(self) -> None:
        result = get_repo_name()
        expected = "python-utilities"
        assert result == expected


class TestGetRepoRoot:
    @given(root=git_repos())
    @settings_with_reduced_examples()
    def test_main(self, *, root: Path) -> None:
        result = get_repo_root(cwd=root)
        expected = root.resolve()
        assert result == expected

    def test_error(self, *, tmp_path: Path) -> None:
        with raises(
            GetRepoRootError, match="Path is not part of a `git` repository: .*"
        ):
            _ = get_repo_root(cwd=tmp_path)
