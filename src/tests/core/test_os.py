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
        key, value = [unique_str() for _ in range(2)]
        with yield_temp_environ({key: value}):
            assert get_env(key) == value

    def test_case_sensitive(self) -> None:
        key, value = [unique_str() for _ in range(2)]
        with (
            yield_temp_environ({key.lower(): value}),
            raises(GetEnvError, match=r"No environment variable '.*'"),
        ):
            _ = get_env(key.upper(), case_sensitive=True)

    @given(
        data=data(),
        key=text.map(_prefix),
        value=text,
        default=text | none(),
        nullable=booleans(),
    )
    def test_case_insensitive(
        self,
        *,
        data: DataObject,
        key: str,
        value: str,
        default: str | None,
        nullable: bool,
    ) -> None:
        key_use = data.draw(sampled_from([key, key.lower(), key.upper()]))
        with yield_temp_environ({key: value}):
            result = get_env(key_use, default=default, nullable=nullable)
        assert result == value

    @given(
        key=text.map(_prefix),
        case_sensitive=booleans(),
        default=text,
        nullable=booleans(),
    )
    def test_default(
        self, *, key: str, case_sensitive: bool, default: str, nullable: bool
    ) -> None:
        value = get_env(
            key, case_sensitive=case_sensitive, default=default, nullable=nullable
        )
        assert value == default

    @given(key=text.map(_prefix), case_sensitive=booleans())
    def test_nullable(self, *, key: str, case_sensitive: bool) -> None:
        value = get_env(key, case_sensitive=case_sensitive, nullable=True)
        assert value is None

    @given(key=text.map(_prefix), case_sensitive=booleans())
    def test_error(self, *, key: str, case_sensitive: bool) -> None:
        with raises(GetEnvError, match=r"No environment variable .*(\(modulo case\))?"):
            _ = get_env(key, case_sensitive=case_sensitive)

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
