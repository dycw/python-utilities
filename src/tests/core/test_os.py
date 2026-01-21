from __future__ import annotations

from os import getenv
from typing import TYPE_CHECKING

from hypothesis import HealthCheck, Phase, given, reproduce_failure, settings
from hypothesis.strategies import DataObject, booleans, data, none, sampled_from
from pytest import RaisesGroup, approx, fixture, mark, param, raises, skip

from utilities.contextvars import set_global_breakpoint
from utilities.core import GetEnvError, get_env, unique_str, yield_temp_environ
from utilities.hypothesis import text_ascii

text = text_ascii(min_size=1, max_size=10)


def _prefix(text: str, /) -> str:
    return f"_TEST_OS_{text}"


class TestGetEnv:
    def test_main(self) -> None:
        key, value = self._generate()
        with yield_temp_environ({key: value}):
            assert get_env(key) == value

    def test_case_insensitive(self) -> None:
        key, value = self._generate()
        with yield_temp_environ({key.lower(): value}):
            assert get_env(key.upper()) == value

    def test_default(self) -> None:
        key, value = self._generate()
        assert get_env(key, default=value) == value

    def test_nullable(self) -> None:
        key, _ = self._generate()
        assert get_env(key, nullable=True) is None

    def test_error_case_insensitive(self) -> None:
        key1, value = self._generate()
        key2, _ = self._generate()
        with (
            yield_temp_environ({key1: value}),
            raises(GetEnvError, match=r"No environment variable '.*' \(modulo case\)"),
        ):
            _ = get_env(key2)

    def test_error_case_sensitive(self) -> None:
        key, value = self._generate()
        with (
            yield_temp_environ({key.lower(): value}),
            raises(GetEnvError, match=r"No environment variable '.*'"),
        ):
            _ = get_env(key.upper(), case_sensitive=True)

    def _generate(self) -> tuple[str, str]:
        key = f"_TEST_OS_{unique_str()}"
        value = unique_str()
        return key, value

    def _prefix(self, text: str, /) -> str:
        return f"_TEST_OS_{text}"


class TestYieldTempEnviron:
    @given(key=text.map(_prefix), value=text)
    def test_set(self, *, key: str, value: str) -> None:
        assert getenv(key) is None
        with yield_temp_environ({key: value}):
            assert getenv(key) == value
        assert getenv(key) is None

    @given(key=text.map(_prefix), prev=text, new=text)
    def test_override(self, *, key: str, prev: str, new: str) -> None:
        with yield_temp_environ({key: prev}):
            assert getenv(key) == prev
            with yield_temp_environ({key: new}):
                assert getenv(key) == new
            assert getenv(key) == prev

    @given(key=text.map(_prefix), value=text)
    def test_unset(self, *, key: str, value: str) -> None:
        with yield_temp_environ({key: value}):
            assert getenv(key) == value
            with yield_temp_environ({key: None}):
                assert getenv(key) is None
            assert getenv(key) == value
