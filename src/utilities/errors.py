from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, assert_never, override

if TYPE_CHECKING:
    from utilities.types import ExceptionTypeLike, MaybeType


@dataclass(kw_only=True, slots=True)
class ImpossibleCaseError(Exception):
    case: list[str]

    @override
    def __str__(self) -> str:
        desc = ", ".join(self.case)
        return f"Case must be possible: {desc}."
        ##


##


def is_instance_error(
    error: BaseException, class_or_tuple: ExceptionTypeLike[Exception], /
) -> bool:
    """Check if an instance relationship holds, allowing for groups."""
    if isinstance(error, class_or_tuple):
        return True
    if not isinstance(error, BaseExceptionGroup):
        return False
    return any(is_instance_error(e, class_or_tuple) for e in error.exceptions)


##


def repr_error(error: MaybeType[BaseException], /) -> str:
    """Get a string representation of an error."""
    match error:
        case ExceptionGroup() as group:
            descs = list(map(repr_error, group.exceptions))
            joined = ", ".join(descs)
            return f"{group.__class__.__name__}({joined})"
        case BaseException() as error_obj:
            return f"{error_obj.__class__.__name__}({error_obj})"
        case type() as error_cls:
            return error_cls.__name__
        case _ as never:
            assert_never(never)


__all__ = ["ImpossibleCaseError", "is_instance_error", "repr_error"]
