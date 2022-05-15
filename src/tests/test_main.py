from dycw_utilities import __version__


def test_version() -> None:
    assert isinstance(__version__, str)
