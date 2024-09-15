from __future__ import annotations

from time import sleep

from utilities.loguru import LogLevel, log


def func_test_entry_inc_and_dec(x: int, /) -> tuple[int, int]:
    with log():
        inc = _func_test_entry_inc(x)
        dec = _func_test_entry_dec(x)
        return inc, dec


def _func_test_entry_inc(x: int, /) -> int:
    with log():
        return x + 1


def _func_test_entry_dec(x: int, /) -> int:
    with log():
        return x - 1


def func_test_entry_disabled(x: int, /) -> int:
    with log(entry_level=None):
        return x + 1


def func_test_entry_non_default_level(x: int, /) -> int:
    with log(entry_level=LogLevel.DEBUG):
        return x + 1


def func_test_error(x: int, /) -> int | None:
    with log():
        if x % 2 == 0:
            return x + 1
        msg = f"Got an odd number: {x}"
        raise ValueError(msg)


def func_test_error_expected(x: int, /) -> int | None:
    with log(error_expected=ValueError):
        if x % 2 == 0:
            return x + 1
        msg = f"Got an odd number: {x}"
        raise ValueError(msg)


def func_test_exit_explicit(x: int, /) -> int:
    with log(exit_level=LogLevel.DEBUG):
        return x + 1


def func_test_exit_duration(x: int, /) -> int:
    with log(exit_duration=0.01):
        sleep(0.02)
        return x + 1
