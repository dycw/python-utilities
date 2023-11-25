from __future__ import annotations

from os import getenv

from hypothesis import given

from utilities.hypothesis import text_ascii
from utilities.os import CPU_COUNT, get_cpu_count, temp_environ

text = text_ascii(min_size=1, max_size=10)


class TestCPUCount:
    def test_function(self) -> None:
        assert isinstance(get_cpu_count(), int)

    def test_constant(self) -> None:
        assert isinstance(CPU_COUNT, int)


def prefix(text: str, /) -> str:
    return f"_TEST_OS_{text}"


class TestTempEnviron:
    @given(key=text.map(prefix), value=text)
    def test_set(self, *, key: str, value: str) -> None:
        assert getenv(key) is None
        with temp_environ({key: value}):
            assert getenv(key) == value
        assert getenv(key) is None

    @given(key=text.map(prefix), prev=text, new=text)
    def test_override(self, *, key: str, prev: str, new: str) -> None:
        with temp_environ({key: prev}):
            assert getenv(key) == prev
            with temp_environ({key: new}):
                assert getenv(key) == new
            assert getenv(key) == prev

    @given(key=text.map(prefix), value=text)
    def test_unset(self, *, key: str, value: str) -> None:
        with temp_environ({key: value}):
            assert getenv(key) == value
            with temp_environ({key: None}):
                assert getenv(key) is None
            assert getenv(key) == value
