from __future__ import annotations

from hypothesis import given
from hypothesis.strategies import integers
from pytest import mark, param, raises

from utilities.core import yield_temp_environ
from utilities.os import GetCPUUseError, get_cpu_use, is_debug, is_pytest


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


class TestIsDebug:
    @mark.parametrize("env_var", [param("DEBUG"), param("debug")])
    def test_main(self, *, env_var: str) -> None:
        with yield_temp_environ({env_var: "1"}):
            assert is_debug()

    def test_off(self) -> None:
        with yield_temp_environ(DEBUG=None, debug=None):
            assert not is_debug()


class TestIsPytest:
    def test_main(self) -> None:
        assert is_pytest()

    def test_off(self) -> None:
        with yield_temp_environ(PYTEST_VERSION=None):
            assert not is_pytest()
