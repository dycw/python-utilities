from __future__ import annotations

from typing import TYPE_CHECKING

from click import argument, command, echo, option

from utilities.click import ListInts
from utilities.text import join_strs

if TYPE_CHECKING:
    from collections.abc import Iterable


from beartype import beartype


@beartype
def _serialize_iterable_ints(values: list[int], /) -> str:
    return join_strs(map(str, values))


@command()
@option("--value", type=ListInts(separator=","), default=[])
@beartype
def main(*, value: list[int]) -> None:
    assert 0, type(value)
    echo(f"value = {_serialize_iterable_ints(value)}")


if __name__ == "__main__":
    main()
