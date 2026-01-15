from __future__ import annotations

from pathlib import Path
from typing import Annotated, assert_never

from pydantic import BeforeValidator, SecretStr

from utilities.types import PathLike

type ExpandedPath = Annotated[PathLike, BeforeValidator(lambda p: Path(p).expanduser())]
type SecretLike = SecretStr | str


def extract_secret(value: SecretLike, /) -> str:
    """Given a secret, extract its value."""
    match value:
        case SecretStr():
            return value.get_secret_value()
        case str():
            return value
        case never:
            assert_never(never)


__all__ = ["ExpandedPath", "SecretLike", "extract_secret"]
