from __future__ import annotations

from utilities.rich import yield_call_args_repr, yield_mapping_repr


class TestYieldCallArgsRepr:
    def test_main(self) -> None:
        lines = list(yield_call_args_repr(1, 2, 3, x=4, y=5, z=6))
        expected = [
            "args[0] = 1",
            "args[1] = 2",
            "args[2] = 3",
            "kwargs[x] = 4",
            "kwargs[y] = 5",
            "kwargs[z] = 6",
        ]
        assert lines == expected


class TestYieldMappingRepr:
    def test_main(self) -> None:
        lines = list(yield_mapping_repr(a=1, b=2, c=3))
        expected = ["a = 1", "b = 2", "c = 3"]
        assert lines == expected
