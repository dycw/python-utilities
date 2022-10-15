from random import choice
from typing import Any
from typing import cast

from attrs import define
from attrs import fields
from beartype import beartype
from beartype.door import die_if_unbearable


@define
class AttrsBase:
    """Base class for `attrs` class which applies `beartype` checking."""

    @beartype
    def __attrs_post_init__(self) -> None:
        all_fields = fields(cast(Any, type(self)))
        try:
            field = choice(all_fields)  # noqa: S311
        except IndexError:
            pass
        else:
            die_if_unbearable(getattr(self, field.name), field.type)
