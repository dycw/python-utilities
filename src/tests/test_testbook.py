from __future__ import annotations

from json import dumps
from typing import TYPE_CHECKING

from pytest import mark, param

from utilities.testbook import build_test_class
from utilities.whenever import HOUR

if TYPE_CHECKING:
    from pathlib import Path


class TestBuildTestClass:
    @mark.parametrize("throttle", [param(HOUR), param(None)])
    def test_main(self, *, tmp_path: Path) -> None:
        data = {"cells": []}
        _ = tmp_path.joinpath("notebook.ipynb").write_text(dumps(data))
        _ = build_test_class(tmp_path)
