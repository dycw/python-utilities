from __future__ import annotations

from pathlib import Path

from pytest import mark

from utilities.core import normalize_multi_line_str, repr_mapping, repr_str, repr_table


class TestReprMapping:
    @mark.xfail
    def test_main(self) -> None:
        mapping = {"a": 1, "b": 2, "c": 3, "d": list(range(100))}
        result = repr_mapping(mapping)
        expected = [
            "a = 1",
            "b = 2",
            "c = 3",
            "d = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, ... +80]",
        ]
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
