from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from dacite import Config, from_dict
from hypothesis import given
from hypothesis.strategies import none, sampled_from

from utilities.dacite import yield_literal_forward_references

type TruthLit = Literal["true", "false"]


class TestYieldLiteralForwardReferences:
    @given(truth=sampled_from(["true", "false"]))
    def test_literal(self, *, truth: Literal["true", "false"]) -> None:
        @dataclass(kw_only=True, slots=True)
        class Example:
            truth: Literal["true", "false"]

        data = {"truth": truth}
        fwd_refs = dict(yield_literal_forward_references(Example, globalns=globals()))
        result = from_dict(Example, data, config=Config(forward_references=fwd_refs))
        expected = Example(truth=truth)
        assert result == expected

    @given(truth=sampled_from(["true", "false"]) | none())
    def test_literal_nullable(self, *, truth: Literal["true", "false"] | None) -> None:
        @dataclass(kw_only=True, slots=True)
        class Example:
            truth: Literal["true", "false"] | None = None

        data = {"truth": truth}
        fwd_refs = dict(yield_literal_forward_references(Example, globalns=globals()))
        result = from_dict(Example, data, config=Config(forward_references=fwd_refs))
        expected = Example(truth=truth)
        assert result == expected

    @given(truth=sampled_from(["true", "false"]))
    def test_literal_type(self, *, truth: TruthLit) -> None:
        @dataclass(kw_only=True, slots=True)
        class Example:
            truth: TruthLit

        data = {"truth": truth}
        fwd_refs = dict(yield_literal_forward_references(Example, globalns=globals()))
        result = from_dict(Example, data, config=Config(forward_references=fwd_refs))
        expected = Example(truth=truth)
        assert result == expected

    @given(truth=sampled_from(["true", "false"]) | none())
    def test_literal_type_nullable(self, *, truth: TruthLit | None) -> None:
        @dataclass(kw_only=True, slots=True)
        class Example:
            truth: TruthLit | None = None

        data = {"truth": truth}
        fwd_refs = dict(yield_literal_forward_references(Example, globalns=globals()))
        result = from_dict(Example, data, config=Config(forward_references=fwd_refs))
        expected = Example(truth=truth)
        assert result == expected
