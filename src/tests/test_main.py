from git.repo import Repo

from dycw_template import __version__


def test_version() -> None:
    assert isinstance(__version__, str)


def test_repo() -> None:
    assert isinstance(Repo("."), Repo)
