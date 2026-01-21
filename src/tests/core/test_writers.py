from __future__ import annotations

from utilities.core import yield_write_path


class TestYieldWritePath:
    def test_main(self, *, tmp_path: Path) -> None:
        path = tmp_path / "file.txt"
        with yield_write_path(path) as temp:
            assert not path.exists()
            _ = temp.write_text("text")
