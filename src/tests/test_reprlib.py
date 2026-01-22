from __future__ import annotations

from utilities.reprlib import yield_mapping_repr


class TestYieldMappingRepr:
    def test_main(self) -> None:
        mapping = {"a": 1, "b": 2, "c": 3, "d": list(range(100))}
        lines = list(yield_mapping_repr(mapping))
        expected = [
            "a = 1",
            "b = 2",
            "c = 3",
            "d = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, ... +80]",
        ]
        assert lines == expected
