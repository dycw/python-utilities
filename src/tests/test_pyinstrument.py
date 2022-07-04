from pathlib import Path
from re import search
from time import sleep

from utilities.pyinstrument import profile


class TestProfile:
    def test_main(self, tmp_path: Path) -> None:
        with profile(path=tmp_path):
            sleep(1e-3)

        (file,) = tmp_path.iterdir()
        assert search(r"^profile__\d{8}T\d{6}\.html$", file.name)
