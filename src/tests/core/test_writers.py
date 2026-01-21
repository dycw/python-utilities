from __future__ import annotations

from typing import TYPE_CHECKING

from utilities.core import yield_write_path

if TYPE_CHECKING:
    from pathlib import Path


class TestYieldWritePath:
    def test_main(self, *, tmp_path: Path) -> None:
        path = tmp_path / "file.txt"
        with yield_write_path(path) as temp:
            _ = temp.write_text("text")
            assert not path.exists()
        assert path.is_file()
        assert path.read_text() == "text"

    def test_overwrite(self, *, tmp_path: Path) -> None:
        path = tmp_path / "file.txt"
        with yield_write_path(path) as temp:
            _ = temp.write_text("text")
            assert not path.exists()
        assert path.is_file()
        assert path.read_text() == "text"
