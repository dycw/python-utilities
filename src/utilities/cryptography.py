from __future__ import annotations

from dataclasses import dataclass
from os import getenv
from typing import override

from cryptography.fernet import Fernet

_ENV_VAR = "FERNET_KEY"


def encrypt(text: str, /, *, env_var: str = _ENV_VAR) -> bytes:
    """Encrypt a string."""
    return get_fernet(env_var=env_var).encrypt(text.encode())


def decrypt(text: bytes, /, *, env_var: str = _ENV_VAR) -> str:
    """Encrypt a string."""
    return get_fernet(env_var=env_var).decrypt(text).decode()


##


def get_fernet(*, env_var: str = _ENV_VAR) -> Fernet:
    """Get the Fernet key."""
    if (key := getenv(env_var)) is None:
        raise GetFernetError(env_var=env_var)
    return Fernet(key.encode())


@dataclass(kw_only=True, slots=True)
class GetFernetError(Exception):
    env_var: str

    @override
    def __str__(self) -> str:
        return f"Environment variable {self.env_var!r} is None"


__all__ = ["GetFernetError", "decrypt", "encrypt", "get_fernet"]
