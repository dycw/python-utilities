from __future__ import annotations

from subprocess import PIPE
from subprocess import check_output

from hypothesis import given
from hypothesis.strategies import DataObject
from hypothesis.strategies import data
from hypothesis.strategies import none

from utilities.git import _GET_BRANCH_NAME
from utilities.hypothesis import git_repos
from utilities.hypothesis import text_ascii
from utilities.tempfile import TemporaryDirectory


class TestGitRepos:
    @given(data=data())
    def test_fixed(self, *, data: DataObject) -> None:
        branch = data.draw(text_ascii(min_size=1) | none())
        repo = data.draw(git_repos(branch=branch))
        assert isinstance(repo, TemporaryDirectory)
        path = repo.path
        assert set(path.iterdir()) == {path.joinpath(".git")}
        if branch is not None:
            output = check_output(
                _GET_BRANCH_NAME, stderr=PIPE, cwd=path, text=True  # noqa: S603
            )
            assert output.strip("\n") == branch
