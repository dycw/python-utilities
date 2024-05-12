from __future__ import annotations

from click import argument, command

from utilities.click import ListInts


@command()
@argument("foo", type=ListInts(separator=","))
def main(foo) -> None:
    assert 0, f"""
You passed utilities.pandas import IndexS

foo = {foo}

with types

types = {list(map(type, foo))}
    """


if __name__ == "__main__":
    main()
