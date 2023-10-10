from __future__ import annotations

from pathlib import Path

from hypothesis import given
from hypothesis.strategies import DataObject
from hypothesis.strategies import data
from pytest import raises

from utilities.git import InvalidRepoError
from utilities.git import get_branch_name
from utilities.git import get_repo_name
from utilities.git import get_repo_root
from utilities.hypothesis import git_repos
from utilities.hypothesis import text_ascii
from utilities.tempfile import TemporaryDirectory


class TestGetBranchName:
    @given(data=data(), branch=text_ascii(min_size=1))
    def test_main(self, *, data: DataObject, branch: str) -> None:
        repo = data.draw(git_repos(branch=branch))
        result = get_branch_name(cwd=repo.path)
        assert result == branch


class TestGetRepoName:
    def test_main(self) -> None:
        result = get_repo_name()
        expected = "python-utilities"
        assert result == expected


class TestGetRepoRoot:
    @given(repo=git_repos())
    def test_main(self, *, repo: TemporaryDirectory) -> None:
        path = repo.path
        result = get_repo_root(cwd=path)
        assert result == path.resolve()

    def test_error(self, *, tmp_path: Path) -> None:
        with raises(InvalidRepoError):
            _ = get_repo_root(cwd=tmp_path)
