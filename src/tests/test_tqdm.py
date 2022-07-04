from typing import Any

from tqdm import tqdm as _tqdm

from utilities.tqdm import tqdm


class TestTqdm:
    def test_tqdm(self, capsys: Any) -> None:
        _ = list(tqdm(range(10)))
        assert capsys.readouterr().err == ""

    def test_native(self, capsys: Any) -> None:
        _ = list(_tqdm(range(10)))
        assert capsys.readouterr().err != ""
