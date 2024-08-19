from __future__ import annotations

from pathlib import Path

from hypothesis import given
from hypothesis.strategies import integers, sets

from utilities.hypothesis import temp_paths
from utilities.pathlib import list_dir, temp_cwd


class TestListDir:
    @given(root=temp_paths(), nums=sets(integers(0, 100), max_size=10))
    def test_main(self, *, root: Path, nums: set[str]) -> None:
        for n in nums:
            path = root.joinpath(f"{n}.txt")
            path.touch()
        result = list_dir(root)
        expected = sorted(Path(root, f"{n}.txt") for n in nums)
        assert result == expected


class TestTempCWD:
    def test_main(self, *, tmp_path: Path) -> None:
        assert Path.cwd() != tmp_path
        with temp_cwd(tmp_path):
            assert Path.cwd() == tmp_path
        assert Path.cwd() != tmp_path
