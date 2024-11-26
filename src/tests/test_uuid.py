from __future__ import annotations

from typing import TYPE_CHECKING

from hypothesis import given
from hypothesis.strategies import uuids

from utilities.uuid import UUID_PATTERN

if TYPE_CHECKING:
    from uuid import UUID


class TestUUIDPattern:
    @given(uuid=uuids())
    def test_main(self, *, uuid: UUID) -> None:
        assert UUID_PATTERN.match(str(uuid))
