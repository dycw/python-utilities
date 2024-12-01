from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from utilities.dacite import yield_literal_forward_references

type TruthLit = Literal["true", "false"]


class TestGenerateForwardReferences:
    def test_main(self) -> None:
        @dataclass(kw_only=True, slots=True)
        class Example:
            truth: TruthLit

        result = list(yield_literal_forward_references(Example, globalns=globals()))
        expected = [("TruthLit", Literal["true", "false"])]
        assert result == expected
