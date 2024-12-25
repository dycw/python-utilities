from __future__ import annotations

from pathlib import Path
from re import search

from pytest import raises

from utilities.git import (
    GetRepoRootError,
    get_branch_name,
    get_ref_tags,
    get_repo_name,
    get_repo_root,
)
from utilities.iterables import one


class TestGetBranchName:
    def test_main(self) -> None:
        name = get_branch_name()
        assert search(r"\w+", name)


class TestGetRefTags:
    def test_main(self) -> None:
        tag = one(get_ref_tags("origin/master"))
        assert search(r"^\d+\.\d+\.\d+$", tag)


class TestGetRepoName:
    def test_main(self) -> None:
        result = get_repo_name()
        expected = "python-utilities"
        assert result == expected


class TestGetRepoRoot:
    def test_main(self) -> None:
        root = get_repo_root()
        assert isinstance(root, Path)

    def test_error(self, *, tmp_path: Path) -> None:
        with raises(
            GetRepoRootError, match="Path is not part of a `git` repository: .*"
        ):
            _ = get_repo_root(cwd=tmp_path)
