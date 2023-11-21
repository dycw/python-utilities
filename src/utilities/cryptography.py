from __future__ import annotations

from os import getenv

from cryptography.fernet import Fernet

_ENV_VAR = "FERNET_KEY"


def encrypt(text: str, /, *, env_var: str = _ENV_VAR) -> bytes:
    """Encrypt a string."""
    return _get_fernet(env_var=env_var).encrypt(text.encode())


def decrypt(text: bytes, /, *, env_var: str = _ENV_VAR) -> str:
    """Encrypt a string."""
    return _get_fernet(env_var=env_var).decrypt(text).decode()


def _get_fernet(*, env_var: str = _ENV_VAR) -> Fernet:
    """Get the Fernet key."""
    if (key := getenv(env_var)) is None:
        msg = f"{env_var!r} is None"
        raise FernetKeyMissingError(msg)
    return Fernet(key.encode())


class FernetKeyMissingError(Exception):
    """Raised when the fernet key is missing."""


__all__ = ["decrypt", "encrypt", "FernetKeyMissingError"]
