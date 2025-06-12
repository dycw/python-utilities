from __future__ import annotations

from re import search
from time import sleep
from typing import TYPE_CHECKING

from utilities.pyinstrument import profile

if TYPE_CHECKING:
    from pathlib import Path


class TestProfile:
    def test_main(self, tmp_path: Path) -> None:
        with profile(path=tmp_path):
            sleep(1e-3)

        (file,) = tmp_path.iterdir()
        assert search(r"^profile__[\dT]+\.html$", file.name)
