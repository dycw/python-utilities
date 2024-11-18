from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from typing import Protocol, runtime_checkable

from beartype import beartype

from utilities.traceback import trace

_ = dt


@beartype
@dataclass(order=True, unsafe_hash=True, kw_only=True)
class Bar:
    """A (non-nullable) bar."""

    start: dt.datetime
    end: dt.datetime

    @property
    def duration(self) -> dt.timedelta:
        return self.end - self.start


@runtime_checkable
class _HasDatetime(Protocol):
    @property
    def datetime(self) -> Bar: ...


@trace
@beartype
def check_bar_duration(data: Bar | _HasDatetime, duration: dt.timedelta, /) -> None:
    """Check that a bar has the required duration."""
    if isinstance(data, Bar):
        if data.duration == duration:
            return
        msg = f"duration={data.duration}, expected={duration}"
        raise ValueError(msg)
    check_bar_duration(data.datetime, duration)
