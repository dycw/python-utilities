from __future__ import annotations

from typing import TYPE_CHECKING, Any

from hypothesis import given
from hypothesis.strategies import DataObject, data, sampled_from
from pytest import mark, param, raises

from utilities.sentinel import (
    SENTINEL_REPR,
    ParseSentinelError,
    Sentinel,
    is_sentinel,
    parse_sentinel,
    sentinel,
)

if TYPE_CHECKING:
    from collections.abc import Callable


class TestIsSentinel:
    @mark.parametrize(("obj", "expected"), [param(None, False), param(sentinel, True)])
    def test_main(self, *, obj: Any, expected: bool) -> None:
        assert is_sentinel(obj) is expected


class TestParseSentinel:
    @given(data=data())
    def test_main(self, *, data: DataObject) -> None:
        text = str(sentinel)
        text_use = data.draw(sampled_from(["", text, text.lower(), text.upper()]))
        result = parse_sentinel(text_use)
        assert result is sentinel

    @given(text=sampled_from(["invalid", "ssentinell"]))
    def test_error(self, *, text: str) -> None:
        with raises(
            ParseSentinelError, match=r"Unable to parse sentinel value; got '.*'"
        ):
            _ = parse_sentinel(text)


class TestSentinel:
    def test_isinstance(self) -> None:
        assert isinstance(sentinel, Sentinel)

    @mark.parametrize("method", [param(repr), param(str)])
    def test_repr_and_str(self, method: Callable[..., str]) -> None:
        assert method(sentinel) == SENTINEL_REPR

    def test_singletone(self) -> None:
        assert Sentinel() is sentinel
