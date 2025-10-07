from __future__ import annotations

from pathlib import Path
from typing import Annotated

from boltons.fileutils import FilePerms
from pydantic import BeforeValidator

from utilities.iterables import one
from utilities.re import ExtractGroupError, extract_group
from utilities.text import parse_bool
from utilities.types import PathLike

ExpandedPath = Annotated[Path, BeforeValidator(lambda p: Path(p).expanduser())]


__all__ = ["ExpandedPath"]
