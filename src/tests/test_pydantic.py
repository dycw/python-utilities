from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from pydantic import BaseModel


class TestExpandedPath:
    from utilities.pydantic import ExpandedPath

    class Example(BaseModel):
        path: ExpandedPath

    _ = Example.model_rebuild()

    result = Example(path=Path("~")).path
    expected = Path.home()
    assert result == expected
