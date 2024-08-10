from __future__ import annotations

from asyncio import sleep
from typing import TYPE_CHECKING

from utilities.tqdm import tqdm_async

if TYPE_CHECKING:
    from collections.abc import AsyncIterator


class TestTqdmAsyncio:
    async def test_main(self) -> None:
        async def yield_ints() -> AsyncIterator[int]:
            for i in range(5):
                yield i
                await sleep(0.1)

        ints = [i async for i in tqdm_async(yield_ints())]
        assert len(ints) == 5
        for i in ints:
            assert isinstance(i, int)
