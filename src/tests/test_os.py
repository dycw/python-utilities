from __future__ import annotations

from os import getenv
from typing import TYPE_CHECKING

from hypothesis import given
from hypothesis.strategies import (
    DataObject,
    booleans,
    data,
    integers,
    none,
    sampled_from,
)
from pytest import mark, param, raises

from utilities.hypothesis import text_ascii
from utilities.os import (
    CPU_COUNT,
    EFFECTIVE_GROUP_ID,
    EFFECTIVE_USER_ID,
    GetCPUUseError,
    GetEnvVarError,
    get_cpu_count,
    get_cpu_use,
    get_effective_group_id,
    get_effective_user_id,
    get_env_var,
    is_debug,
    is_pytest,
    temp_environ,
)
from utilities.pytest import skipif_windows

if TYPE_CHECKING:
    from collections.abc import Callable

text = text_ascii(min_size=1, max_size=10)


def _prefix(text: str, /) -> str:
    return f"_TEST_OS_{text}"


class TestGetCPUCount:
    def test_function(self) -> None:
        assert isinstance(get_cpu_count(), int)

    def test_constant(self) -> None:
        assert isinstance(CPU_COUNT, int)


class TestGetCPUUse:
    @given(n=integers(min_value=1))
    def test_int(self, *, n: int) -> None:
        result = get_cpu_use(n=n)
        assert result == n

    def test_all(self) -> None:
        result = get_cpu_use(n="all")
        assert isinstance(result, int)
        assert result >= 1

    @given(n=integers(max_value=0))
    def test_error(self, *, n: int) -> None:
        with raises(GetCPUUseError, match=r"Invalid number of CPUs to use: -?\d+"):
            _ = get_cpu_use(n=n)


class TestGetEffectiveIDs:
    @mark.parametrize(
        "func", [param(get_effective_user_id), param(get_effective_group_id)]
    )
    def test_function(self, *, func: Callable[[], int | None]) -> None:
        id_ = func()
        assert isinstance(id_, int) or (id_ is None)

    @mark.parametrize("id_", [param(EFFECTIVE_USER_ID), param(EFFECTIVE_GROUP_ID)])
    def test_constant(self, *, id_: int | None) -> None:
        assert isinstance(id_, int) or (id_ is None)


class TestGetEnvVar:
    @given(
        key=text.map(_prefix), value=text, default=text | none(), nullable=booleans()
    )
    @skipif_windows
    def test_case_sensitive(
        self, *, key: str, value: str, default: str | None, nullable: bool
    ) -> None:
        with temp_environ({key: value}):
            result = get_env_var(key, default=default, nullable=nullable)
        assert result == value

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
        with temp_environ({key: value}):
            result = get_env_var(key_use, default=default, nullable=nullable)
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
        value = get_env_var(
            key, case_sensitive=case_sensitive, default=default, nullable=nullable
        )
        assert value == default

    @given(key=text.map(_prefix), case_sensitive=booleans())
    def test_nullable(self, *, key: str, case_sensitive: bool) -> None:
        value = get_env_var(key, case_sensitive=case_sensitive, nullable=True)
        assert value is None

    @given(key=text.map(_prefix), case_sensitive=booleans())
    def test_error(self, *, key: str, case_sensitive: bool) -> None:
        with raises(
            GetEnvVarError, match=r"No environment variable .*(\(modulo case\))?"
        ):
            _ = get_env_var(key, case_sensitive=case_sensitive)


class TestIsDebug:
    @mark.parametrize("env_var", [param("DEBUG"), param("debug")])
    def test_main(self, *, env_var: str) -> None:
        with temp_environ({env_var: "1"}):
            assert is_debug()

    def test_off(self) -> None:
        with temp_environ(DEBUG=None, debug=None):
            assert not is_debug()


class TestIsPytest:
    def test_main(self) -> None:
        assert is_pytest()

    def test_off(self) -> None:
        with temp_environ(PYTEST_VERSION=None):
            assert not is_pytest()


class TestTempEnviron:
    @given(key=text.map(_prefix), value=text)
    def test_set(self, *, key: str, value: str) -> None:
        assert getenv(key) is None
        with temp_environ({key: value}):
            assert getenv(key) == value
        assert getenv(key) is None

    @given(key=text.map(_prefix), prev=text, new=text)
    def test_override(self, *, key: str, prev: str, new: str) -> None:
        with temp_environ({key: prev}):
            assert getenv(key) == prev
            with temp_environ({key: new}):
                assert getenv(key) == new
            assert getenv(key) == prev

    @given(key=text.map(_prefix), value=text)
    def test_unset(self, *, key: str, value: str) -> None:
        with temp_environ({key: value}):
            assert getenv(key) == value
            with temp_environ({key: None}):
                assert getenv(key) is None
            assert getenv(key) == value
