from __future__ import annotations


class TestYieldAdjacentTempFile:
    def test_main(self, *, tmp_path: Path) -> None:
        with yield_adjacent_temp_file(tmp_path) as temp:
            self._run_test(tmp_path, temp)

    def test_deep(self, *, tmp_path: Path) -> None:
        path = tmp_path / "a/b/c/file.txt"
        with yield_adjacent_temp_file(path) as temp:
            self._run_test(path, temp)

    def _run_test(self, path: Path, temp: Path, /) -> None:
        assert temp.is_file()
        assert temp.parent == path.parent
        assert temp.name.startswith(path.name)
