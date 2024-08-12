from __future__ import annotations

from hypothesis import given
from hypothesis.strategies import integers, none
from pandas import get_option
from polars import Config

from utilities.jupyter import _DEFAULT_COLS, _DEFAULT_ROWS, is_jupyter, show


class TestIsJupyter:
    def test_main(self) -> None:
        assert not is_jupyter()


class TestShow:
    def test_direct(self) -> None:
        expected = {
            "set_fmt_float": "mixed",
            "set_float_precision": None,
            "set_thousands_separator": "",
            "set_decimal_separator": ".",
            "set_trim_decimal_zeros": False,
        }

        def assert_default() -> None:
            assert get_option("display.precision") == 6
            assert get_option("display.min_rows") == 10
            assert get_option("display.max_rows") == 60
            assert get_option("display.max_columns") == 0
            assert Config.state(if_set=True) == expected

        assert_default()
        with show:
            assert get_option("display.precision") == 6
            assert get_option("display.min_rows") == _DEFAULT_ROWS
            assert get_option("display.max_rows") == _DEFAULT_ROWS
            assert get_option("display.max_columns") == _DEFAULT_COLS
            assert Config.state(if_set=True) == expected | {
                "POLARS_FMT_MAX_ROWS": str(_DEFAULT_ROWS),
                "POLARS_FMT_MAX_COLS": str(_DEFAULT_COLS),
            }
        assert_default()

    @given(
        rows=integers(0, 100) | none(),
        dp=integers(0, 10) | none(),
        columns=integers(0, 100) | none(),
    )
    def test_indirect(
        self, *, rows: int | None, dp: int | None, columns: int | None
    ) -> None:
        with show(rows, dp=dp, columns=columns):
            state = Config.state(if_set=True)
            if dp is not None:
                assert get_option("display.precision") == dp
                assert state["set_float_precision"] == dp
            if rows is not None:
                assert get_option("display.min_rows") == rows
                assert get_option("display.max_rows") == rows
                assert state["POLARS_FMT_MAX_ROWS"] == str(rows)
            if columns is not None:
                assert get_option("display.max_columns") == columns
                assert state["POLARS_FMT_MAX_COLS"] == str(columns)
