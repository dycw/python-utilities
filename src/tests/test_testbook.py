from __future__ import annotations

from typing import TYPE_CHECKING

from utilities.testbook import build_test_class

if TYPE_CHECKING:
    from pathlib import Path


class TestBuildTestClass:
    def test_main(self, *, tmp_path: Path) -> None:
        tmp_path.joinpath("notebook.ipynb").touch()
        build_test_class(tmp_path)
