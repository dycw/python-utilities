from __future__ import annotations

from pathlib import Path
from typing import Annotated

from pydantic import BeforeValidator

ExpandedPath = Annotated[Path, BeforeValidator(lambda p: Path(p).expanduser())]


__all__ = ["ExpandedPath"]
