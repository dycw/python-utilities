from __future__ import annotations

from re import search
from typing import TYPE_CHECKING

from hypothesis import given
from hypothesis.strategies import uuids

from utilities.uuid import UUID_EXACT_PATTERN, UUID_PATTERN

if TYPE_CHECKING:
    from uuid import UUID


class TestUUIDPattern:
    @given(uuid=uuids())
    def test_main(self, *, uuid: UUID) -> None:
        assert search(UUID_PATTERN, str(uuid))

    @given(uuid=uuids())
    def test_exact(self, *, uuid: UUID) -> None:
        text = f".{uuid}."
        assert search(UUID_PATTERN, text)
        assert not search(UUID_EXACT_PATTERN, text)
