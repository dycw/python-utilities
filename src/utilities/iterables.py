from __future__ import annotations

from collections import Counter
from collections.abc import Hashable, Iterable, Sized
from dataclasses import dataclass
from typing import Any, TypeGuard

from typing_extensions import override

from utilities.math import is_equal_or_approx
from utilities.types import ensure_hashable


def check_duplicates(iterable: Iterable[Hashable], /) -> None:
    """Check if an iterable contains any duplicates."""
    dup = {k: v for k, v in Counter(iterable).items() if v > 1}
    if len(dup) >= 1:
        msg = f"{dup=}"
        raise CheckDuplicatesError(msg)


class CheckDuplicatesError(Exception):
    ...


def check_length(
    obj: Sized,
    /,
    *,
    equal: int | None = None,
    equal_or_approx: int | tuple[int, float] | None = None,
    min: int | None = None,  # noqa: A002
    max: int | None = None,  # noqa: A002
) -> None:
    """Check the length of an object."""
    if (equal is not None) and (len(obj) != equal):
        raise _CheckLengthEqualError(obj=obj, equal=equal)
    if (equal_or_approx is not None) and not is_equal_or_approx(
        len(obj), equal_or_approx
    ):
        raise _CheckLengthEqualOrApproxError(obj=obj, equal_or_approx=equal_or_approx)
    if (min is not None) and (len(obj) < min):
        raise _CheckLengthMinError(obj=obj, min_=min)
    if (max is not None) and (len(obj) > max):
        raise _CheckLengthMaxError(obj=obj, max_=max)


class CheckLengthError(Exception):
    ...


@dataclass(frozen=True, kw_only=True, slots=True)
class _CheckLengthEqualError(CheckLengthError):
    obj: Sized
    equal: int

    @override
    def __str__(self) -> str:
        return "Object {} must have length {}; got {} instead".format(
            self.obj, self.equal, len(self.obj)
        )


@dataclass(frozen=True, kw_only=True, slots=True)
class _CheckLengthEqualOrApproxError(CheckLengthError):
    obj: Sized
    equal_or_approx: int | tuple[int, float]

    @override
    def __str__(self) -> str:
        match self.equal_or_approx:
            case target, error:
                desc = "approximate length {} (error {:%})".format(target, error)
            case target:
                desc = "length {}".format(target)
        return "Object {} must have {}; got {} instead".format(
            self.obj, desc, len(self.obj)
        )


@dataclass(frozen=True, kw_only=True, slots=True)
class _CheckLengthMinError(CheckLengthError):
    obj: Sized
    min_: int

    @override
    def __str__(self) -> str:
        return "Object {} must have minimum length {}; got {} instead".format(
            self.obj, self.min_, len(self.obj)
        )


@dataclass(frozen=True, kw_only=True, slots=True)
class _CheckLengthMaxError(CheckLengthError):
    obj: Sized
    max_: int

    @override
    def __str__(self) -> str:
        return "Object {} must have maximum length {}; got {} instead".format(
            self.obj, self.max_, len(self.obj)
        )


def ensure_hashables(
    *args: Any, **kwargs: Any
) -> tuple[list[Hashable], dict[str, Hashable]]:
    """Ensure a set of positional & keyword arguments are all hashable."""
    hash_args = list(map(ensure_hashable, args))
    hash_kwargs = {k: ensure_hashable(v) for k, v in kwargs.items()}
    return hash_args, hash_kwargs


def is_iterable_not_str(obj: Any, /) -> TypeGuard[Iterable[Any]]:
    """Check if an object is iterable, but not a string."""
    try:
        iter(obj)
    except TypeError:
        return False
    return not isinstance(obj, str)


__all__ = [
    "CheckDuplicatesError",
    "check_duplicates",
    "ensure_hashables",
    "is_iterable_not_str",
]
