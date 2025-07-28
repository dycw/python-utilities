from __future__ import annotations

from dataclasses import dataclass, field
from re import search
from typing import TYPE_CHECKING, Self
from uuid import UUID

from hypothesis import given
from hypothesis.strategies import integers, none, randoms, uuids

from utilities.dataclasses import replace_non_sentinel
from utilities.hypothesis import pairs
from utilities.sentinel import Sentinel, sentinel
from utilities.uuid import UUID_EXACT_PATTERN, UUID_PATTERN, get_uuid, to_uuid

if TYPE_CHECKING:
    from random import Random

    from utilities.sentinel import Sentinel
    from utilities.types import MaybeCallableUUIDLike


class TestGetUUID:
    @given(seed=randoms() | none())
    def test_main(self, *, seed: Random | None) -> None:
        assert isinstance(get_uuid(seed), UUID)

    @given(seed=integers())
    def test_deterministic(self, *, seed: int) -> None:
        uuid1, uuid2 = [get_uuid(seed) for _ in range(2)]
        assert uuid1 == uuid2


class TestToUUID:
    def test_default(self) -> None:
        assert isinstance(to_uuid(), UUID)

    @given(uuid=uuids())
    def test_uuid(self, *, uuid: UUID) -> None:
        assert to_uuid(uuid) == uuid

    @given(uuid=uuids())
    def test_str(self, *, uuid: UUID) -> None:
        assert to_uuid(str(uuid)) == uuid

    @given(seed=randoms())
    def test_seed(self, *, seed: Random) -> None:
        assert isinstance(to_uuid(seed), UUID)

    @given(uuid=uuids())
    def test_callable(self, *, uuid: UUID) -> None:
        assert to_uuid(lambda: uuid) == uuid

    def test_none(self) -> None:
        assert isinstance(to_uuid(None), UUID)

    def test_sentinel(self) -> None:
        assert to_uuid(sentinel) is sentinel

    @given(uuids=pairs(uuids()))
    def test_replace_non_sentinel(self, *, uuids: tuple[UUID, UUID]) -> None:
        uuid1, uuid2 = uuids

        @dataclass(kw_only=True, slots=True)
        class Example:
            uuid: UUID = field(default_factory=get_uuid)

            def replace(
                self, *, uuid: MaybeCallableUUIDLike | Sentinel = sentinel
            ) -> Self:
                return replace_non_sentinel(self, uuid=to_uuid(uuid))

        obj = Example(uuid=uuid1)
        assert obj.uuid == uuid1
        assert obj.replace().uuid == uuid1
        assert obj.replace(uuid=uuid2).uuid == uuid2
        assert isinstance(obj.replace(uuid=get_uuid).uuid, UUID)


class TestUUIDPattern:
    @given(uuid=uuids())
    def test_main(self, *, uuid: UUID) -> None:
        assert search(UUID_PATTERN, str(uuid))

    @given(uuid=uuids())
    def test_exact(self, *, uuid: UUID) -> None:
        text = f".{uuid}."
        assert search(UUID_PATTERN, text)
        assert not search(UUID_EXACT_PATTERN, text)
