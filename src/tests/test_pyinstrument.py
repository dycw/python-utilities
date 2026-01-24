from __future__ import annotations

from re import search
from typing import TYPE_CHECKING

from utilities.core import sync_sleep
from utilities.pyinstrument import profile

if TYPE_CHECKING:
    from pathlib import Path


class TestProfile:
    def test_main(self, tmp_path: Path) -> None:
        with profile(tmp_path):
            sync_sleep(0.1)

        (file,) = tmp_path.iterdir()
        assert search(r"^profile__.+?\.html$", file.name)
