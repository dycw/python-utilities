from __future__ import annotations

from typing import TYPE_CHECKING

from utilities.lzma import lzma_paths, yield_lzma_contents

if TYPE_CHECKING:
    from pathlib import Path


class TestLZMAPathsAndYieldLZMAContents:
    def test_single_file(self, *, tmp_path: Path, temp_file: Path) -> None:
        _ = temp_file.write_text("text")
        dest = tmp_path / "dest"
        lzma_paths(temp_file, dest)
        with yield_lzma_contents(dest) as temp:
            assert temp.is_file()
            assert temp.read_text() == "text"
