from __future__ import annotations

from typing import ClassVar

from utilities.inspect import yield_object_attributes


class TestYieldObjectAttributes:
    def test_main(self) -> None:
        class Example:
            attr: ClassVar[int] = 1

        attrs = dict(yield_object_attributes(Example))
        assert len(attrs) == 29
        assert attrs["attr"] == 1
