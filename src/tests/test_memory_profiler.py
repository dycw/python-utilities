from __future__ import annotations

from hypothesis import given
from hypothesis.strategies import integers

from utilities.hypothesis import settings_with_reduced_examples
from utilities.memory_profiler import memory_profiled


def func(n: int, /) -> list[int]:
    return list(range(n))


class TestMemoryProfiled:
    @given(n=integers(1, 10))
    @settings_with_reduced_examples()
    def test_main(self, *, n: int) -> None:
        profiled = memory_profiled(func)
        result = profiled(n)
        assert result.value == list(range(n))
        assert isinstance(result.memory, float)
