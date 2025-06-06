from __future__ import annotations

from typing import TYPE_CHECKING

from hypothesis import given, settings
from pytest import raises

from utilities.git import GetRepoRootError, get_repo_root
from utilities.hypothesis import git_repos

if TYPE_CHECKING:
    from pathlib import Path


class TestGetRepoRoot:
    @given(repo=git_repos())
    @settings(max_examples=1)
    def test_main(self, *, repo: Path) -> None:
        root = get_repo_root(path=repo)
        expected = repo.resolve()
        assert root == expected

    def test_error(self, *, tmp_path: Path) -> None:
        with raises(
            GetRepoRootError, match="Path is not part of a `git` repository: .*"
        ):
            _ = get_repo_root(path=tmp_path)
