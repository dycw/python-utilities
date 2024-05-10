from __future__ import annotations


def func1(x: int, /) -> int:
    y = 2
    return func2(x, y)


def func2(x: int, y: int, /) -> int:
    z = 3
    return func3(x, y, z)


def func3(x: int, y: int, z: int, /) -> int:
    return (x + y + z) // 0
