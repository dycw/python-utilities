from __future__ import annotations

from collections import Counter
from collections.abc import Hashable, Iterable, Iterator, Mapping, Sized
from collections.abc import Set as AbstractSet
from dataclasses import dataclass
from enum import Enum, auto
from typing import Any, TypeGuard, cast

from typing_extensions import Never, assert_never, override

from utilities.errors import ImpossibleCaseError
from utilities.math import is_equal_or_approx
from utilities.more_itertools import one
from utilities.text import ensure_str
from utilities.types import ensure_hashable


def check_duplicates(iterable: Iterable[Hashable], /) -> None:
    """Check if an iterable contains any duplicates."""
    counts = {k: v for k, v in Counter(iterable).items() if v > 1}
    if len(counts) >= 1:
        raise CheckDuplicatesError(iterable=iterable, counts=counts)


@dataclass(frozen=True, kw_only=True, slots=True)
class CheckDuplicatesError(Exception):
    iterable: Iterable[Hashable]
    counts: dict[Hashable, int]

    @override
    def __str__(self) -> str:
        return "Iterable {} must not contain duplicates; got {}".format(
            self.iterable, ", ".join(f"({k}, n={v})" for k, v in self.counts.items())
        )


class _CheckIterablesEqualState(Enum):
    left_longer = auto()
    right_longer = auto()


def check_iterables_equal(left: Iterable[Any], right: Iterable[Any], /) -> None:
    """Check that a pair of iterables are equal."""

    errors: list[tuple[int, Any, Any]] = []
    state: _CheckIterablesEqualState | None
    try:
        for i, (lv, rv) in enumerate(zip(left, right, strict=True)):
            if lv != rv:
                errors.append((i, lv, rv))
    except ValueError as error:
        msg = ensure_str(one(error.args))
        match msg:
            case "zip() argument 2 is longer than argument 1":
                state = _CheckIterablesEqualState.right_longer
            case "zip() argument 2 is shorter than argument 1":
                state = _CheckIterablesEqualState.left_longer
            case _:  # pragma: no cover
                raise ImpossibleCaseError(  # pragma: no cover
                    case=[f"{msg=}"]
                ) from None
    else:
        state = None
    if (len(errors) >= 1) or (state is not None):
        raise CheckIterablesEqualError(
            left=left, right=right, errors=errors, state=state
        )


@dataclass(frozen=True, kw_only=True, slots=True)
class CheckIterablesEqualError(Exception):
    left: Iterable[Any]
    right: Iterable[Any]
    errors: list[tuple[int, Any, Any]]
    state: _CheckIterablesEqualState | None

    @override
    def __str__(self) -> str:
        parts = list(self._yield_parts())
        match parts:
            case (desc,):
                pass
            case first, second:
                desc = "{} and {}".format(first, second)
            case _ as never:  # pragma: no cover
                assert_never(cast(Never, never))
        return "Iterables {} and {} must be equal; {}".format(
            self.left, self.right, desc
        )

    def _yield_parts(self) -> Iterator[str]:
        if len(self.errors) >= 1:
            error_descs = (
                "({}, {}, i={})".format(lv, rv, i) for i, lv, rv in self.errors
            )
            yield "items were {}".format(", ".join(error_descs))
        match self.state:
            case _CheckIterablesEqualState.left_longer:
                yield "left was longer"
            case _CheckIterablesEqualState.right_longer:
                yield "right was longer"
            case None:
                pass
            case _ as never:  # type: ignore
                assert_never(never)


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


@dataclass(frozen=True, kw_only=True, slots=True)
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


def check_mappings_equal(left: Mapping[Any, Any], right: Mapping[Any, Any], /) -> None:
    """Check that a pair of mappings are equal."""
    left_keys, right_keys = set(left), set(right)
    try:
        check_sets_equal(left_keys, right_keys)
    except CheckSetsEqualError as error:
        left_extra, right_extra = map(set, [error.left_extra, error.right_extra])
    else:
        left_extra = right_extra = set()
    errors: list[tuple[Any, Any, Any]] = []
    for key in left_keys & right_keys:
        lv, rv = left[key], right[key]
        if lv != rv:
            errors.append((key, lv, rv))
    if (len(left_extra) >= 1) or (len(right_extra) >= 1) or (len(errors) >= 1):
        raise CheckMappingsEqualError(
            left=left,
            right=right,
            left_extra=left_extra,
            right_extra=right_extra,
            errors=errors,
        )


@dataclass(frozen=True, kw_only=True, slots=True)
class CheckMappingsEqualError(Exception):
    left: Mapping[Any, Any]
    right: Mapping[Any, Any]
    left_extra: AbstractSet[Any]
    right_extra: AbstractSet[Any]
    errors: list[tuple[Any, Any, Any]]

    @override
    def __str__(self) -> str:
        parts = list(self._yield_parts())
        match parts:
            case (desc,):
                pass
            case first, second:
                desc = "{} and {}".format(first, second)
            case first, second, third:
                desc = "{}, {} and {}".format(first, second, third)
            case _ as never:  # pragma: no cover
                assert_never(cast(Never, never))
        return "Mappings {} and {} must be equal; {}".format(
            self.left, self.right, desc
        )

    def _yield_parts(self) -> Iterator[str]:
        if len(self.left_extra) >= 1:
            yield "left had extra keys {}".format(self.left_extra)
        if len(self.right_extra) >= 1:
            yield "right had extra keys {}".format(self.right_extra)
        if len(self.errors) >= 1:
            error_descs = (
                "({}, {}, k={})".format(lv, rv, k) for k, lv, rv in self.errors
            )
            yield "items were {}".format(", ".join(error_descs))


def check_sets_equal(left: AbstractSet[Any], right: AbstractSet[Any], /) -> None:
    """Check that a pair of sets are equal."""
    left_extra = left - right
    right_extra = right - left
    if (len(left_extra) >= 1) or (len(right_extra) >= 1):
        raise CheckSetsEqualError(
            left=left, right=right, left_extra=left_extra, right_extra=right_extra
        )


@dataclass(frozen=True, kw_only=True, slots=True)
class CheckSetsEqualError(Exception):
    left: AbstractSet[Any]
    right: AbstractSet[Any]
    left_extra: AbstractSet[Any]
    right_extra: AbstractSet[Any]

    @override
    def __str__(self) -> str:
        parts = list(self._yield_parts())
        match parts:
            case (desc,):
                pass
            case first, second:
                desc = "{} and {}".format(first, second)
            case _ as never:  # pragma: no cover
                assert_never(cast(Never, never))
        return "Sets {} and {} must be equal; {}".format(self.left, self.right, desc)

    def _yield_parts(self) -> Iterator[str]:
        if len(self.left_extra) >= 1:
            yield "left had extra items {}".format(self.left_extra)
        if len(self.right_extra) >= 1:
            yield "right had extra items {}".format(self.right_extra)


def check_subset(left: AbstractSet[Any], right: AbstractSet[Any], /) -> None:
    """Check that a set is a subset of another set."""
    extra = left - right
    if len(extra) >= 1:
        raise CheckSubsetError(left=left, right=right, extra=extra)


@dataclass(frozen=True, kw_only=True, slots=True)
class CheckSubsetError(Exception):
    left: AbstractSet[Any]
    right: AbstractSet[Any]
    extra: AbstractSet[Any]

    @override
    def __str__(self) -> str:
        return "Set {} must be a subset of {}; left had extra items {}".format(
            self.left, self.right, self.extra
        )


def check_superset(left: AbstractSet[Any], right: AbstractSet[Any], /) -> None:
    """Check that a set is a superset of another set."""
    extra = right - left
    if len(extra) >= 1:
        raise CheckSupersetError(left=left, right=right, extra=extra)


@dataclass(frozen=True, kw_only=True, slots=True)
class CheckSupersetError(Exception):
    left: AbstractSet[Any]
    right: AbstractSet[Any]
    extra: AbstractSet[Any]

    @override
    def __str__(self) -> str:
        return "Set {} must be a superset of {}; right had extra items {}".format(
            self.left, self.right, self.extra
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
    "CheckIterablesEqualError",
    "CheckLengthsEqualError",
    "CheckMappingsEqualError",
    "CheckSetsEqualError",
    "CheckSubsetError",
    "CheckSupersetError",
    "check_duplicates",
    "check_iterables_equal",
    "check_lengths_equal",
    "check_mappings_equal",
    "check_sets_equal",
    "check_subset",
    "check_superset",
    "ensure_hashables",
    "is_iterable_not_str",
]
