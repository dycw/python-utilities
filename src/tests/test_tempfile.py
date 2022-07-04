from pathlib import Path

from utilities.tempfile import TemporaryDirectory
from utilities.tempfile import gettempdir


class TestGetTempDir:
    def test_main(self) -> None:
        assert isinstance(gettempdir(), Path)


class TestTemporaryDirectory:
    def test_name(self) -> None:
        temp_dir = TemporaryDirectory()
        name = temp_dir.name
        assert isinstance(name, Path)
        assert name.is_dir()
        assert set(name.iterdir()) == set()

    def test_as_context_manager(self) -> None:
        with TemporaryDirectory() as temp:
            assert isinstance(temp, Path)
            assert temp.is_dir()
            assert set(temp.iterdir()) == set()
        assert not temp.is_dir()
