from random import choice
from typing import Any, cast

from attrs import define, fields
from beartype import beartype
from beartype.door import die_if_unbearable
from beartype.roar import BeartypeDoorHintViolation

from utilities.class_name import get_class_name


@define
class AttrsBase:
    """Base class for `attrs` class which applies `beartype` checking."""

    @beartype
    def __attrs_post_init__(self) -> None:
        all_fields = fields(cast(Any, type(self)))
        try:
            field = choice(all_fields)
        except IndexError:
            pass
        else:
            fname = field.name
            try:
                die_if_unbearable(getattr(self, fname), field.type)
            except BeartypeDoorHintViolation:
                msg = (
                    f"module = {self.__module__}, "
                    f"class = {get_class_name(self)}, field = {fname}"
                )
                raise FieldTypeError(msg) from None


class FieldTypeError(TypeError):
    """Raised when an `attrs` field has the wrong type."""
