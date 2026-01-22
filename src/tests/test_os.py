from __future__ import annotations

from hypothesis import given
from hypothesis.strategies import integers
from pytest import raises

from utilities.os import GetCPUUseError, get_cpu_use


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
