from __future__ import annotations

from pathlib import Path

from hypothesis import given
from hypothesis.strategies import booleans, floats, integers, sampled_from
from pytest import raises

from tests.test_operator import TruthEnum
from utilities.hypothesis import paths, text_ascii
from utilities.math import is_equal
from utilities.parse import ParseTextError, parse_text


class TestParseText:
    @given(value=booleans())
    def test_bool(self, *, value: bool) -> None:
        text = str(value)
        result = parse_text(bool, text)
        assert result is value

    @given(truth=sampled_from(TruthEnum))
    def test_enum(self, *, truth: TruthEnum) -> None:
        text = truth.name
        result = parse_text(TruthEnum, text)
        assert result is truth

    @given(value=floats())
    def test_float(self, *, value: float) -> None:
        text = str(value)
        result = parse_text(float, text)
        assert is_equal(result, value)

    @given(value=integers())
    def test_int(self, *, value: int) -> None:
        text = str(value)
        result = parse_text(int, text)
        assert result == value

    @given(path=paths())
    def test_path(self, *, path: Path) -> None:
        text = str(path)
        result = parse_text(Path, text)
        assert result == path

    @given(text=text_ascii())
    def test_str(self, *, text: str) -> None:
        result = parse_text(str, text)
        assert result == text

    def test_error_bool(self) -> None:
        with raises(
            ParseTextError, match="Unable to parse <class 'bool'>; got 'invalid'"
        ):
            _ = parse_text(bool, "invalid")

    def test_error_enum(self) -> None:
        with raises(
            ParseTextError, match="Unable to parse <enum 'TruthEnum'>; got 'invalid'"
        ):
            _ = parse_text(TruthEnum, "invalid")

    def test_error_float(self) -> None:
        with raises(
            ParseTextError, match="Unable to parse <class 'float'>; got 'invalid'"
        ):
            _ = parse_text(float, "invalid")

    def test_error_int(self) -> None:
        with raises(
            ParseTextError, match="Unable to parse <class 'int'>; got 'invalid'"
        ):
            _ = parse_text(int, "invalid")
