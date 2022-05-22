from hypothesis import given
from hypothesis.strategies import integers
from typeguard import typeguard_ignore

from dycw_utilities.memory_profiler import Output
from dycw_utilities.memory_profiler import memory_profiled


@typeguard_ignore
def func(n: int, /) -> list[int]:
    return list(range(n))


class TestMemoryProfiled:
    @given(n=integers(1, int(1e6)))
    def test_main(self, n: int) -> None:
        profiled = memory_profiled(func)
        result = profiled(n)
        assert isinstance(result, Output)
        assert result.value == list(range(n))
        assert result.memory <= (80.0 * n)
