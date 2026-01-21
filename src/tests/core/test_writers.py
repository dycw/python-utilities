from __future__ import annotations

from utilities.core import yield_write_path


class TestYieldWritePath:
    def test_main(self, *, tmp_path: Path) -> None:
        path = tmp_path / "file.txt"
        with yield_write_path(path) as temp:
            _ = temp.write_text("text")
        ass

    def test_deep(self, *, tmp_path: Path) -> None:
        path = tmp_path / "a/b/c/file.txt"
        with yield_adjacent_temp_file(path) as temp:
            self._run_test(path, temp)

    def _run_test(self, path: Path, temp: Path, /) -> None:
        assert temp.is_file()
        assert temp.parent == path.parent
        assert temp.name.startswith(path.name)
