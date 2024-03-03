from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from utilities.sentinel import _REPR, Sentinel, sentinel

if TYPE_CHECKING:
    from collections.abc import Callable


class TestSentinel:
    def test_isinstance(self) -> None:
        assert isinstance(sentinel, Sentinel)

    @pytest.mark.parametrize("method", [pytest.param(repr), pytest.param(str)])
    def test_repr_and_str(self, method: Callable[..., str]) -> None:
        assert method(sentinel) == _REPR

    def test_singletone(self) -> None:
        assert Sentinel() is sentinel
