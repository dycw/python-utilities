from __future__ import annotations

from pathlib import Path

from hypothesis import given, settings

from utilities.git import get_repo
from utilities.hypothesis import git_repos, paths


class TestGetRepo:
    @given(repo=git_repos(), tail=paths())
    @settings(max_examples=1)
    def test_main(self, *, repo: Path, tail: Path) -> None:
        obj = get_repo(repo.joinpath(tail))
        expected = repo.resolve()
        assert obj.working_tree_dir is not None
        root = Path(obj.working_tree_dir).resolve()
        assert root == expected
