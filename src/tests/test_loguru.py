from __future__ import annotations

import datetime as dt
from typing import TYPE_CHECKING

from pytest import mark, param

from utilities.loguru import InterceptHandler, logged_sleep_async, logged_sleep_sync

if TYPE_CHECKING:
    from utilities.types import Duration


class TestInterceptHandler:
    def test_main(self) -> None:
        _ = InterceptHandler()


class TestLoggedSleep:
    @mark.parametrize("duration", [param(0.01), param(dt.timedelta(seconds=0.1))])
    def test_sync(self, *, duration: Duration) -> None:
        logged_sleep_sync(duration)

    @mark.parametrize("duration", [param(0.01), param(dt.timedelta(seconds=0.1))])
    async def test_async(self, *, duration: Duration) -> None:
        await logged_sleep_async(duration)
