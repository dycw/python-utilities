from __future__ import annotations

from re import search
from typing import TYPE_CHECKING

from hypothesis import given
from hypothesis.strategies import sampled_from, uuids

from utilities.uuid import UUID_EXACT_PATTERN, UUID_PATTERN

if TYPE_CHECKING:
    from uuid import UUID


class TestUUIDPattern:
    @given(pattern=sampled_from([UUID_PATTERN, UUID_EXACT_PATTERN]), uuid=uuids())
    def test_main(self, *, pattern: str, uuid: UUID) -> None:
        assert search(pattern, str(uuid))
