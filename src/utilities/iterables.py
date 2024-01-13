from __future__ import annotations

from collections import Counter
from collections.abc import Hashable, Iterable, Sized
from dataclasses import dataclass
from enum import Enum, auto
from typing import Any, TypeGuard

from typing_extensions import assert_never, override

from utilities.errors import ImpossibleCaseError
from utilities.math import is_equal_or_approx
from utilities.more_itertools import one
from utilities.text import ensure_str
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
        return "Object {} must have length {}; got {}".format(
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
        return "Object {} must have {}; got {}".format(self.obj, desc, len(self.obj))


@dataclass(frozen=True, kw_only=True, slots=True)
class _CheckLengthMinError(CheckLengthError):
    obj: Sized
    min_: int

    @override
    def __str__(self) -> str:
        return "Object {} must have minimum length {}; got {}".format(
            self.obj, self.min_, len(self.obj)
        )


@dataclass(frozen=True, kw_only=True, slots=True)
class _CheckLengthMaxError(CheckLengthError):
    obj: Sized
    max_: int

    @override
    def __str__(self) -> str:
        return "Object {} must have maximum length {}; got {}".format(
            self.obj, self.max_, len(self.obj)
        )


def check_lengths_equal(left: Sized, right: Sized, /) -> None:
    """Check that a pair of sizes objects have equal length."""
    if len(left) != len(right):
        raise CheckLengthsEqualError(left=left, right=right)


@dataclass(frozen=True, kw_only=True, slots=True)
class CheckLengthsEqualError(Exception):
    left: Sized
    right: Sized

    @override
    def __str__(self) -> str:
        return (
            "Sized objects {} and {} must have the same length; got {} and {}".format(
                self.left, self.right, len(self.left), len(self.right)
            )
        )


class _CheckListsEqualState(Enum):
    left_longer = auto()
    right_longer = auto()


def check_lists_equal(left: list[Any], right: list[Any], /) -> None:
    """Check that a pair of lists are equal."""

    errors: list[tuple[int, Any, Any]] = []
    state: _CheckListsEqualState | None
    try:
        for i, (lv, rv) in enumerate(zip(left, right, strict=True)):
            if lv != rv:
                errors.append((i, lv, rv))
    except ValueError as error:
        msg = ensure_str(one(error.args))
        match msg:
            case "zip() argument 2 is longer than argument 1":
                state = _CheckListsEqualState.right_longer
            case "zip() argument 2 is shorter than argument 1":
                state = _CheckListsEqualState.left_longer
            case _:  # pragma: no cover
                raise ImpossibleCaseError(  # pragma: no cover
                    case=[f"{msg=}"]
                ) from None
    else:
        state = None
    if (len(errors) >= 1) or (state is not None):
        raise CheckListsEqualError(left=left, right=right, errors=errors, state=state)


@dataclass(frozen=True, kw_only=True, slots=True)
class CheckListsEqualError(Exception):
    left: list[Any]
    right: list[Any]
    errors: list[tuple[int, Any, Any]]
    state: _CheckListsEqualState | None

    @override
    def __str__(self) -> str:
        if len(self.errors) >= 1:
            error_descs = (
                "({}, {}, i={})".format(lv, rv, i) for i, lv, rv in self.errors
            )
            error_desc = "items were {}".format(", ".join(error_descs))
        else:
            error_desc = None
        match self.state:
            case _CheckListsEqualState.left_longer:
                state_desc = "left was longer"
            case _CheckListsEqualState.right_longer:
                state_desc = "right was longer"
            case None:
                state_desc = None
            case _ as never:  # type: ignore
                assert_never(never)
        if (error_desc is not None) and (state_desc is not None):
            desc = "{}, and {}".format(error_desc, state_desc)
        elif (error_desc is not None) and (state_desc is None):
            desc = error_desc
        elif (error_desc is None) and (state_desc is not None):
            desc = state_desc
        else:
            raise ImpossibleCaseError(  # pragma: no cover
                case=[f"{error_desc=}", f"{state_desc=}"]
            )
        return "Lists {} and {} must be equal; {}".format(self.left, self.right, desc)


def check_sets_equal(left: set[Any], right: set[Any], /) -> None:
    """Check that a pair of sets are equal."""
    left_extra = left - right
    right_extra = right - left
    if (len(left_extra) >= 1) or (len(right_extra) >= 1):
        raise CheckSetsEqualError(
            left=left, right=right, left_extra=left_extra, right_extra=right_extra
        )


@dataclass(frozen=True, kw_only=True, slots=True)
class CheckSetsEqualError(Exception):
    left: set[Any]
    right: set[Any]
    left_extra: set[Any]
    right_extra: set[Any]

    @override
    def __str__(self) -> str:
        if len(self.left_extra) >= 1:
            left_desc = "left had extra items {}".format(self.left_extra)
        else:
            left_desc = None
        if len(self.right_extra) >= 1:
            right_desc = "right had extra items {}".format(self.right_extra)
        else:
            right_desc = None
        if (left_desc is not None) and (right_desc is not None):
            desc = "{} and {}".format(left_desc, right_desc)
        elif (left_desc is not None) and (right_desc is None):
            desc = left_desc
        elif (left_desc is None) and (right_desc is not None):
            desc = right_desc
        else:
            raise ImpossibleCaseError(  # pragma: no cover
                case=[f"{left_desc=}", f"{right_desc=}"]
            )
        return "Sets {} and {} must be equal; {}".format(self.left, self.right, desc)


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
    "CheckLengthsEqualError",
    "CheckListsEqualError",
    "CheckSetsEqualError",
    "check_duplicates",
    "check_lengths_equal",
    "check_lists_equal",
    "check_sets_equal",
    "ensure_hashables",
    "is_iterable_not_str",
]
