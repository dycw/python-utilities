from __future__ import annotations

from utilities.rich import yield_pretty_repr_args_and_kwargs


class TestYieldPrettyReprArgsAndKwargs:
    def test_main(self) -> None:
        lines = list(yield_pretty_repr_args_and_kwargs(1, 2, 3, x=4, y=5, z=6))
        expected = [
            "args[0] = 1",
            "args[1] = 2",
            "args[2] = 3",
            "kwargs[x] = 4",
            "kwargs[y] = 5",
            "kwargs[z] = 6",
        ]
        assert lines == expected
