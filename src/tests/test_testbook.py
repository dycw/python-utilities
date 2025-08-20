from __future__ import annotations

from json import dumps
from typing import TYPE_CHECKING

from pytest import mark, param

from utilities.testbook import build_notebook_tester
from utilities.whenever import HOUR

if TYPE_CHECKING:
    from pathlib import Path

    from utilities.types import Delta


class TestBuildNotebookTester:
    @mark.parametrize("throttle", [param(HOUR), param(None)])
    def test_main(self, *, tmp_path: Path, throttle: Delta | None) -> None:
        data = {"cells": []}
        _ = tmp_path.joinpath("notebook.ipynb").write_text(dumps(data))
        _ = build_notebook_tester(tmp_path, throttle=throttle)
