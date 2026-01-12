from __future__ import annotations

from utilities.tabulate import params_table


class TestParamsTable:
    def test_main(self) -> None:
        x = 1
        y = 2
        result = params_table(f"{x=}", f"{y=}")
        expected = 1
        assert result == expected
