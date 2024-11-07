from __future__ import annotations

from tenacity import retry, wait_fixed

from utilities.tenacity import before_sleep_log

# tenacity


_counter = 0


@retry(wait=wait_fixed(0.01), before_sleep=before_sleep_log())
def func_test_tenacity_before_sleep_log() -> int:
    global _counter  # noqa: PLW0603
    _counter += 1
    if _counter >= 3:
        return _counter
    raise ValueError(_counter)
