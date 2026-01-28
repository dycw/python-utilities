from __future__ import annotations

from pathlib import Path

from utilities.core import repr_str


class TestReprStr:
    def test_main(self) -> None:
        assert repr_str(Path("path")) == "'path'"
