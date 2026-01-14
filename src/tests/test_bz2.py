from __future__ import annotations

from typing import TYPE_CHECKING

from utilities.bz2 import bz2_paths, yield_bz2_contents

if TYPE_CHECKING:
    from pathlib import Path


class TestBZ2PathsAndYieldBZ2Contents:
    def test_single_file(self, *, tmp_path: Path, temp_file: Path) -> None:
        _ = temp_file.write_text("text")
        dest = tmp_path / "dest"
        bz2_paths(temp_file, dest)
        with yield_bz2_contents(dest) as temp:
            assert temp.is_file()
            assert temp.read_text() == "text"
