from __future__ import annotations

from collections.abc import Callable
from random import Random
from typing import TYPE_CHECKING, assert_never, overload
from uuid import UUID, uuid4

from utilities.random import get_state
from utilities.sentinel import Sentinel, sentinel

if TYPE_CHECKING:
    from utilities.types import MaybeCallableUUIDLike, Seed


UUID_PATTERN = "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"
UUID_EXACT_PATTERN = f"^{UUID_PATTERN}$"


##


def get_uuid(seed: Seed | None = None, /) -> UUID:
    """Generate a UUID, possibly with a seed."""
    if seed is None:
        return uuid4()
    state = get_state(seed=seed)
    return UUID(int=state.getrandbits(128), version=4)


##


@overload
def to_uuid(uuid: MaybeCallableUUIDLike, /) -> UUID: ...
@overload
def to_uuid(uuid: None, /) -> None: ...
@overload
def to_uuid(uuid: Sentinel, /) -> Sentinel: ...
def to_uuid(
    uuid: MaybeCallableUUIDLike | None | Sentinel = sentinel, /
) -> UUID | None | Sentinel:
    """Convert to a UUID."""
    match uuid:
        case UUID() | None | Sentinel():
            return uuid
        case str():
            return UUID(uuid)
        case int() | float() | bytes() | bytearray() | Random() as seed:
            return get_uuid(seed)
        case Callable() as func:
            return to_uuid(func())
        case never:
            assert_never(never)


__all__ = ["UUID_EXACT_PATTERN", "UUID_PATTERN", "get_uuid", "to_uuid"]
