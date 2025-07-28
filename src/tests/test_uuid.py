from __future__ import annotations

from dataclasses import dataclass, field
from re import search
from typing import TYPE_CHECKING, Self
from uuid import UUID

from hypothesis import given
from hypothesis.strategies import integers, none, randoms, uuids

from utilities.dataclasses import replace_non_sentinel
from utilities.hypothesis import pairs, sentinels
from utilities.uuid import UUID_EXACT_PATTERN, UUID_PATTERN, generate_uuid, to_uuid

if TYPE_CHECKING:
    from random import Random

    from utilities.sentinel import Sentinel
    from utilities.types import MaybeCallableUUIDLike


class TestGenerateUUID:
    @given(seed=randoms() | none())
    def test_main(self, *, seed: Random | None) -> None:
        uuid = generate_uuid(seed)
        assert isinstance(uuid, UUID)

    @given(seed=integers())
    def test_deterministic(self, *, seed: int) -> None:
        uuid1, uuid2 = [generate_uuid(seed) for _ in range(2)]
        assert uuid1 == uuid2


class TestToUUID:
    @given(uuid=uuids())
    def test_uuid(self, *, uuid: UUID) -> None:
        assert to_uuid(uuid) == uuid

    @given(uuid=uuids())
    def test_str(self, *, uuid: UUID) -> None:
        assert to_uuid(str(uuid)) == uuid

    @given(uuid=none() | sentinels())
    def test_none_or_sentinel(self, *, uuid: None | Sentinel) -> None:
        assert to_uuid(uuid) is uuid

    @given(seed=randoms() | none())
    def test_seed(self, *, seed: Random | None) -> None:
        uuid = to_uuid(seed)
        assert isinstance(uuid, UUID)

    @given(dates=pairs(uuids()))
    def test_replace_non_sentinel(self, *, uuids: tuple[UUID, UUID]) -> None:
        uuid1, uuid2 = uuids

        @dataclass(kw_only=True, slots=True)
        class Example:
            uuid: UUID = field(default_factory=generate_uuid)

            def replace(
                self, *, uuid: MaybeCallableUUIDLike | Sentinel = generate_uuid
            ) -> Self:
                return replace_non_sentinel(self, uuid=to_uuid(uuid))

        obj = Example(uuid=uuid1)
        assert obj.uuid == uuid1
        assert obj.replace().uuid == uuid1
        assert obj.replace(uuid=uuid2).uuid == uuid2
        assert isinstance(obj.replace(uuid=generate_uuid).uuid, UUID)


class TestUUIDPattern:
    @given(uuid=uuids())
    def test_main(self, *, uuid: UUID) -> None:
        assert search(UUID_PATTERN, str(uuid))

    @given(uuid=uuids())
    def test_exact(self, *, uuid: UUID) -> None:
        text = f".{uuid}."
        assert search(UUID_PATTERN, text)
        assert not search(UUID_EXACT_PATTERN, text)
