from __future__ import annotations

from pathlib import Path
from typing import Annotated, assert_never

from pydantic import BeforeValidator, SecretStr

from utilities.types import PathLike, SecretLike

type ExpandedPath = Annotated[PathLike, BeforeValidator(lambda p: Path(p).expanduser())]


def ensure_secret(value: SecretLike, /) -> SecretStr:
    """Given a string, ensure it is wrapped as a secret."""
    match value:
        case SecretStr():
            return value
        case str():
            return SecretStr(value)
        case never:
            assert_never(never)


def extract_secret(value: SecretLike, /) -> str:
    """Given a secret, extract its value."""
    match value:
        case SecretStr():
            return value.get_secret_value()
        case str():
            return value
        case never:
            assert_never(never)


__all__ = ["ExpandedPath", "ensure_secret", "extract_secret"]
