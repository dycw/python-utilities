from pathlib import Path

from dycw_utilities.tempfile import TemporaryDirectory


def test_temporary_directory() -> None:
    with TemporaryDirectory() as temp:
        assert isinstance(temp, Path)
        assert temp.is_dir()
        assert set(temp.iterdir()) == set()
    assert not temp.is_dir()
