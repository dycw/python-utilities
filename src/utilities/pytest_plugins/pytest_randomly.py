from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from random import Random


try:
    from pytest import fixture
except ModuleNotFoundError:
    pass
else:

    @fixture
    def random_state(*, seed: int) -> Random:
        """Fixture for a random state."""
        from utilities.random import get_state

        return get_state(seed=seed)


__all__ = ["random_state"]
