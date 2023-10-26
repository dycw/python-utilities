from __future__ import annotations

from subprocess import PIPE, check_output

from hypothesis import given
from hypothesis.strategies import DataObject, data, none

from utilities.git import _GET_BRANCH_NAME
from utilities.hypothesis import git_repos, text_ascii


class TestGitRepos:
    @given(data=data())
    def test_fixed(self, *, data: DataObject) -> None:
        branch = data.draw(text_ascii(min_size=1) | none())
        path = data.draw(git_repos(branch=branch))
        assert set(path.iterdir()) == {path.joinpath(".git")}
        if branch is not None:
            output = check_output(
                _GET_BRANCH_NAME,  # noqa: S603
                stderr=PIPE,
                cwd=path,
                text=True,
            )
            assert output.strip("\n") == branch
