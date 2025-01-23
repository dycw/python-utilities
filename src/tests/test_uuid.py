from __future__ import annotations

from re import search
from typing import TYPE_CHECKING
from uuid import UUID

from hypothesis import given
from hypothesis.strategies import integers, none, randoms, uuids

from utilities.uuid import UUID_EXACT_PATTERN, UUID_PATTERN, get_uuid

if TYPE_CHECKING:
    from random import Random


class TestGetUUID4:
    @given(seed=randoms() | none())
    def test_main(self, *, seed: Random | None) -> None:
        uuid = get_uuid(seed=seed)
        assert isinstance(uuid, UUID)

    @given(seed=integers())
    def test_deterministic(self, *, seed: int) -> None:
        uuid1, uuid2 = [get_uuid(seed=seed) for _ in range(2)]
        assert uuid1 == uuid2


class TestUUIDPattern:
    @given(uuid=uuids())
    def test_main(self, *, uuid: UUID) -> None:
        assert search(UUID_PATTERN, str(uuid))

    @given(uuid=uuids())
    def test_exact(self, *, uuid: UUID) -> None:
        text = f".{uuid}."
        assert search(UUID_PATTERN, text)
        assert not search(UUID_EXACT_PATTERN, text)
