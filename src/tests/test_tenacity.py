from __future__ import annotations

from re import search
from typing import TYPE_CHECKING

from loguru import logger
from tenacity import retry, wait_fixed

from utilities.tenacity import before_sleep_log

if TYPE_CHECKING:
    from pathlib import Path


class TestLoguruAdapter:
    def test_main(self, *, tmp_path: Path) -> None:
        _ = logger.add(path := tmp_path.joinpath("file"))

        i = 0

        @retry(wait=wait_fixed(0.01), before_sleep=before_sleep_log())
        def func() -> int:
            nonlocal i
            i += 1
            if i >= 3:
                return i
            raise ValueError(i)

        assert func() == 3
        with path.open() as fh:
            lines = fh.readlines()
        assert len(lines) == 2
        for line in lines:
            assert search(
                r"Retrying tests\.test_tenacity\.TestLoguruAdapter\.test_main\.<locals>\.func in 0.01 seconds as it raised ValueError: \d",
                line,
            )
