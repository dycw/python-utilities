from __future__ import annotations

from pathlib import Path
from typing import Annotated

from pydantic import BeforeValidator, SecretStr

type ExpandedPath = Annotated[Path, BeforeValidator(lambda p: Path(p).expanduser())]
type SecretLike = SecretStr | str


__all__ = ["ExpandedPath", "SecretLike"]
