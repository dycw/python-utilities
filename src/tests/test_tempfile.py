from pathlib import Path

from dycw_utilities.tempfile import TemporaryDirectory


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
