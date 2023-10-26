from __future__ import annotations

from pathlib import Path

from utilities.tempfile import TEMP_DIR, TemporaryDirectory, gettempdir


class TestGetTempDir:
    def test_main(self) -> None:
        assert isinstance(gettempdir(), Path)


class TestTempDir:
    def test_main(self) -> None:
        assert isinstance(TEMP_DIR, Path)


class TestTemporaryDirectory:
    def test_path(self) -> None:
        temp_dir = TemporaryDirectory()
        path = temp_dir.path
        assert isinstance(path, Path)
        assert path.is_dir()
        assert set(path.iterdir()) == set()

    def test_as_context_manager(self) -> None:
        with TemporaryDirectory() as temp:
            assert isinstance(temp, Path)
            assert temp.is_dir()
            assert set(temp.iterdir()) == set()
        assert not temp.is_dir()
