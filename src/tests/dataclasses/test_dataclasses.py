from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from pytest import mark, param

from utilities.dataclasses import is_dataclass_instance


class TestIsDataClassInstance:
    def test_main(self) -> None:
        @dataclass
        class Example:
            x: None = None

        assert not is_dataclass_instance(Example)
        assert is_dataclass_instance(Example())

    @mark.parametrize("obj", [param(None), param(True), param(False)])
    def test_others(self, *, obj: Any) -> None:
        assert not is_dataclass_instance(obj)
