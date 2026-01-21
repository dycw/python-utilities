from __future__ import annotations

from utilities.core import normalize_multi_line_str
from utilities.tabulate import func_param_desc, params_table


class TestFuncParamDesc:
    def test_empty(self) -> None:
        x = 1
        y = 2

        def func() -> None: ...

        result = func_param_desc(func, "0.0.1", f"{x=}", f"{y=}")
        expected = normalize_multi_line_str("""
            Running 'func' (version 0.0.1) with:
              ╭───┬───╮
              │ x │ 1 │
              │ y │ 2 │
              ╰───┴───╯
            """)
        assert result == expected

    def test_main(self) -> None:
        x = 1
        y = 2
        result = params_table(f"{x=}", f"{y=}")
        expected = normalize_multi_line_str("""
            ╭───┬───╮
            │ x │ 1 │
            │ y │ 2 │
            ╰───┴───╯
        """)
        assert result == expected


class TestParamsTable:
    def test_empty(self) -> None:
        assert params_table() == "\n"

    def test_main(self) -> None:
        x = 1
        y = 2
        result = params_table(f"{x=}", f"{y=}")
        expected = normalize_multi_line_str("""
            ╭───┬───╮
            │ x │ 1 │
            │ y │ 2 │
            ╰───┴───╯
        """)
        assert result == expected
