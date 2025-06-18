from __future__ import annotations

from typing import TYPE_CHECKING
from uuid import UUID, uuid4

from utilities.random import get_state

if TYPE_CHECKING:
    from utilities.types import Seed


UUID_PATTERN = "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"
UUID_EXACT_PATTERN = f"^{UUID_PATTERN}$"


def get_uuid(*, seed: Seed | None = None) -> UUID:
    """Generate a UUID, possibly with a seed."""
    if seed is None:
        return uuid4()
    state = get_state(seed=seed)
    return UUID(int=state.getrandbits(128), version=4)


__all__ = ["UUID_EXACT_PATTERN", "UUID_PATTERN", "get_uuid"]
