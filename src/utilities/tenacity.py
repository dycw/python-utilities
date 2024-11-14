from __future__ import annotations

from typing import TYPE_CHECKING

from tenacity import wait_exponential_jitter as _wait_exponential_jitter
from tenacity._utils import MAX_WAIT
from typing_extensions import override

from utilities.datetime import duration_to_float

if TYPE_CHECKING:
    from utilities.types import Duration


class wait_exponential_jitter(_wait_exponential_jitter):  # noqa: N801
    """Subclass of `wait_exponential_jitter` accepting durations."""

    @override
    def __init__(
        self,
        initial: Duration = 1,
        max: Duration = MAX_WAIT,
        exp_base: float = 2,
        jitter: Duration = 1,
    ) -> None:
        super().__init__(
            initial=duration_to_float(initial),
            max=duration_to_float(max),
            exp_base=exp_base,
            jitter=duration_to_float(jitter),
        )


__all__ = ["wait_exponential_jitter"]
