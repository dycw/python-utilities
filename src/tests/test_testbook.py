from __future__ import annotations

from json import dumps
from re import search
from typing import TYPE_CHECKING, ClassVar

from pytest import mark, param

from utilities.functions import get_class_name
from utilities.testbook import build_notebook_tester
from utilities.whenever import HOUR

if TYPE_CHECKING:
    from pathlib import Path

    from utilities.types import Delta


class TestBuildNotebookTester:
    text: ClassVar[str] = dumps({"cells": []})

    def test_main(self, *, tmp_path: Path) -> None:
        mapping = {
            "notebook": "test_notebook",
            "notebook_with_underscores": "test_notebook_with_underscores",
            "notebook-with-dashes": "test_notebook_with_dashes",
        }
        for stem in mapping:
            _ = tmp_path.joinpath(f"{stem}.ipynb").write_text(self.text)
        tester = build_notebook_tester(tmp_path)
        assert search(r"^TestTestMain\d+$", get_class_name(tester))
        for name in mapping.values():
            assert hasattr(tester, name)

    @mark.parametrize("throttle", [param(HOUR), param(None)])
    def test_throttle(self, *, tmp_path: Path, throttle: Delta | None) -> None:
        _ = tmp_path.joinpath("notebook.ipynb").write_text(self.text)
        _ = build_notebook_tester(tmp_path, throttle=throttle)
