from __future__ import annotations

from pathlib import Path
from textwrap import indent

from pytest import raises

from utilities._core_errors import ReprTableHeaderError
from utilities.core import (
    ReprTableItemsError,
    normalize_multi_line_str,
    normalize_str,
    repr_mapping,
    repr_str,
    repr_table,
)


class TestReprMapping:
    def test_main(self) -> None:
        mapping = {"a": 1, "b": 2, "c": 3}
        result = repr_mapping(mapping)
        expected = normalize_multi_line_str("""
            ┌───┬───┐
            │ a │ 1 │
            │ b │ 2 │
            │ c │ 3 │
            └───┴───┘
        """)
        assert result == expected


class TestReprStr:
    def test_main(self) -> None:
        assert repr_str(Path("path")) == "'path'"


class TestReprTable:
    def test_main(self) -> None:
        result = repr_table(("a", 1), ("b", 2), ("c", 3))
        expected = normalize_multi_line_str("""
            ┌───┬───┐
            │ a │ 1 │
            │ b │ 2 │
            │ c │ 3 │
            └───┴───┘
        """)
        assert result == expected

    def test_long_item(self) -> None:
        result = repr_table(("a", 1), ("b", list(range(100))), ("c", 3))
        expected = normalize_multi_line_str("""
            ┌───┬──────────────────────────────────────────────────────────────────────────┐
            │ a │ 1                                                                        │
            │ b │ [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19,   │
            │   │ ... +80]                                                                 │
            │ c │ 3                                                                        │
            └───┴──────────────────────────────────────────────────────────────────────────┘
        """)
        assert result == expected

    def test_nested(self) -> None:
        inner = repr_table(("a", 1), ("b", 2)).rstrip("\n")
        result = repr_table(("c", 3), ("table", inner), ("d", 4))
        expected = normalize_multi_line_str("""
            ┌───────┬───────────┐
            │ c     │ 3         │
            │ table │ ┌───┬───┐ │
            │       │ │ a │ 1 │ │
            │       │ │ b │ 2 │ │
            │       │ └───┴───┘ │
            │ d     │ 4         │
            └───────┴───────────┘
        """)
        assert result == expected

    def test_header(self) -> None:
        result = repr_table(("a", 1), ("b", 2), ("c", 3), header=["key", "value"])
        expected = normalize_multi_line_str("""
            ┏━━━━━┳━━━━━━━┓
            ┃ key ┃ value ┃
            ┡━━━━━╇━━━━━━━┩
            │ a   │ 1     │
            │ b   │ 2     │
            │ c   │ 3     │
            └─────┴───────┘
        """)
        assert result == expected

    def test_show_edge(self) -> None:
        result = repr_table(("a", 1), ("b", 2), ("c", 3), show_edge=False)
        expected = normalize_multi_line_str("""
            a │ 1
            b │ 2
            c │ 3
        """)
        expected = normalize_str(
            "\n".join(f" {line} " for line in expected.splitlines())
        )
        assert result == expected

    def test_show_lines(self) -> None:
        result = repr_table(("a", 1), ("b", 2), ("c", 3), show_lines=True)
        expected = normalize_multi_line_str("""
            ┌───┬───┐
            │ a │ 1 │
            ├───┼───┤
            │ b │ 2 │
            ├───┼───┤
            │ c │ 3 │
            └───┴───┘
        """)
        assert result == expected

    def test_error_items(self) -> None:
        with raises(
            ReprTableItemsError,
            match=r"Items .* must all be of the same length; got 2, 3 and perhaps more",
        ):
            _ = repr_table(("a", 1), ("b", 2, 3))

    def test_error_header(self) -> None:
        with raises(
            ReprTableHeaderError,
            match=r"Header .* must be of the same length as the items; got 3 for the header and 2 for the items",
        ):
            _ = repr_table(("a", 1), header=["b1", "b2", "b3"])
