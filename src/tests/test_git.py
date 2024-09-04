from __future__ import annotations

from pathlib import Path

from hypothesis import given
from hypothesis.strategies import DataObject, data
from pytest import raises

from utilities.git import (
    GetRepoRootError,
    get_branch_name,
    get_repo_name,
    get_repo_root,
    get_repo_root_or_cwd_sub_path,
)
from utilities.hypothesis import (
    git_repos,
    settings_with_reduced_examples,
    temp_paths,
    text_ascii,
)


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


class TestGetRepoRootOrCwdSubPath:
    @given(root=git_repos())
    @settings_with_reduced_examples()
    def test_exists(self, *, root: Path) -> None:
        def get_file(root: Path, /) -> Path:
            return Path(root, "file.txt")

        result = get_repo_root_or_cwd_sub_path(get_file, cwd=root)
        expected = Path(root, "file.txt").resolve()
        assert result == expected

    @given(root=temp_paths())
    def test_does_not_exist(self, *, root: Path) -> None:
        def get_file(root: Path, /) -> Path:
            return Path(root, "file.txt")

        result = get_repo_root_or_cwd_sub_path(get_file, cwd=root)
        assert result is None

    @given(root=temp_paths())
    def test_missing(self, *, root: Path) -> None:
        def get_file_1(root: Path, /) -> Path:
            return Path(root, "file_1.txt")

        def get_file_2(root: Path, /) -> Path:
            return Path(root, "file_2.txt")

        result = get_repo_root_or_cwd_sub_path(
            get_file_1, cwd=root, if_missing=get_file_2
        )
        expected = Path(root, "file_2.txt")
        assert result == expected


class TestValidRepoPath:
    @given(root=git_repos(), file_name=text_ascii())
    @settings_with_reduced_examples()
    def test_main(self, *, root: Path, file_name: str) -> None:
        result = get_repo_root(cwd=root).joinpath(file_name)
        expected = Path(root.resolve(), file_name)
        assert result == expected
