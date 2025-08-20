from __future__ import annotations

from json import dumps
from typing import TYPE_CHECKING, ClassVar

from pytest import mark, param

from utilities.functions import get_class_name
from utilities.testbook import build_notebook_tester
from utilities.whenever import HOUR

if TYPE_CHECKING:
    from pathlib import Path

    from utilities.types import Delta, StrMapping


class TestBuildNotebookTester:
    data: ClassVar[StrMapping] = {"cells": []}
    text: ClassVar[str] = dumps(data)

    def test_main(self, *, tmp_path: Path) -> None:
        _ = tmp_path.joinpath("notebook.ipynb").write_text(self.text)
        tester = build_notebook_tester(tmp_path)
        assert get_class_name(tester) == "123"

    @mark.parametrize("throttle", [param(HOUR), param(None)])
    def test_throttle(self, *, tmp_path: Path, throttle: Delta | None) -> None:
        _ = tmp_path.joinpath("notebook.ipynb").write_text(self.text)
        _ = build_notebook_tester(tmp_path, throttle=throttle)
