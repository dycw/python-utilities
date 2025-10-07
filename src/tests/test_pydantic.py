from __future__ import annotations

from pathlib import Path

from pydantic import BaseModel

from utilities.pydantic import ExpandedPath

_ = ExpandedPath


class TestExpandedPath:
    def test_main(self) -> None:
        class Example(BaseModel):
            path: ExpandedPath

        _ = Example.model_rebuild()

        result = Example(path=Path("~")).path
        expected = Path.home()
        assert result == expected
