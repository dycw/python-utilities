from pathlib import Path
from re import search

from pytest import raises

from utilities.git import InvalidRepoError
from utilities.git import get_branch_name
from utilities.git import get_repo_name
from utilities.git import get_repo_root


class TestGetBranchName:
    def test_main(self) -> None:
        name = get_branch_name()
        assert search("(HEAD|master|dev.*)", name)


class TestGetRepoName:
    def test_main(self) -> None:
        result = get_repo_name()
        expected = "python-utilities"
        assert result == expected


class TestGetRepoRoot:
    def test_main(self) -> None:
        root = get_repo_root()
        assert any(p.is_dir() and p.name == ".git" for p in root.iterdir())

    def test_error(self, tmp_path: Path) -> None:
        with raises(InvalidRepoError):
            _ = get_repo_root(cwd=tmp_path)
