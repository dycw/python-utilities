from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast, overload

from beartype import BeartypeConf, BeartypeStrategy, beartype

from utilities.ipython import is_ipython
from utilities.jupyter import is_jupyter
from utilities.sys import is_pytest

if TYPE_CHECKING:
    from beartype._data.hint.datahinttyping import (
        BeartypeableT,
        BeartypeConfedDecorator,
    )

_STRATEGY = (
    BeartypeStrategy.O1
    if is_pytest() or is_jupyter() or is_ipython()
    else BeartypeStrategy.O0
)
_CONF = BeartypeConf(is_color=is_pytest(), strategy=_STRATEGY)


@overload
def beartype_if_dev(obj: BeartypeableT) -> BeartypeableT:  # type: ignore[]
    ...


@overload
def beartype_if_dev(*, conf: BeartypeConf) -> BeartypeConfedDecorator: ...


beartype_if_dev = cast(Any, beartype(conf=_CONF))


__all__ = ["beartype_if_dev"]
