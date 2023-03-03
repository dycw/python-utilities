from cryptography.fernet import Fernet
from hypothesis import given
from hypothesis.strategies import text
from pytest import raises

from utilities.cryptography import (
    _ENV_VAR,
    FernetKeyMissingError,
    _get_fernet,
    decrypt,
    encrypt,
)
from utilities.os import temp_environ


class TestEncryptAndDecrypt:
    @given(text=text())
    def test_round_trip(self, text: str) -> None:
        key = Fernet.generate_key()
        with temp_environ({_ENV_VAR: key.decode()}):
            assert decrypt(encrypt(text)) == text

    def test_no_env_var(self) -> None:
        with temp_environ({_ENV_VAR: None}), raises(FernetKeyMissingError):
            _ = _get_fernet()
