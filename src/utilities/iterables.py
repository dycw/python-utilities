from __future__ import annotations

from collections import Counter
from collections.abc import Hashable, Iterable, Iterator, Mapping, Sized
from collections.abc import Set as AbstractSet
from dataclasses import dataclass
from enum import Enum, auto
from typing import Any, Generic, TypeGuard, TypeVar, cast

from typing_extensions import Never, assert_never, override

from utilities.errors import ImpossibleCaseError
from utilities.math import (
    _CheckIntegerEqualError,
    _CheckIntegerEqualOrApproxError,
    _CheckIntegerMaxError,
    _CheckIntegerMinError,
    check_integer,
)
from utilities.more_itertools import one
from utilities.text import ensure_str
from utilities.types import ensure_hashable

_K = TypeVar("_K")
_T = TypeVar("_T")
_V = TypeVar("_V")


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
        return "Iterable {} must not contain duplicates; got {}.".format(
            self.iterable, ", ".join(f"({k}, n={v})" for k, v in self.counts.items())
        )


class _CheckIterablesEqualState(Enum):
    left_longer = auto()
    right_longer = auto()


def check_iterables_equal(left: Iterable[Any], right: Iterable[Any], /) -> None:
    """Check that a pair of iterables are equal."""

    left_list, right_list = map(list, [left, right])
    errors: list[tuple[int, Any, Any]] = []
    state: _CheckIterablesEqualState | None
    it = zip(left_list, right_list, strict=True)
    try:
        for i, (lv, rv) in enumerate(it):
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
            left=left_list, right=right_list, errors=errors, state=state
        )


@dataclass(frozen=True, kw_only=True, slots=True)
class CheckIterablesEqualError(Exception, Generic[_T]):
    left: list[_T]
    right: list[_T]
    errors: list[tuple[int, _T, _T]]
    state: _CheckIterablesEqualState | None

    @override
    def __str__(self) -> str:
        match list(self._yield_parts()):
            case (desc,):
                pass
            case first, second:
                desc = "{} and {}".format(first, second)
            case _ as never:  # pragma: no cover
                assert_never(cast(Never, never))
        return "Iterables {} and {} must be equal; {}.".format(
            self.left, self.right, desc
        )

    def _yield_parts(self) -> Iterator[str]:
        if len(self.errors) >= 1:
            error_descs = (
                "({}, {}, i={})".format(lv, rv, i) for i, lv, rv in self.errors
            )
            yield "differing items were {}".format(", ".join(error_descs))
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
    n = len(obj)
    try:
        check_integer(n, equal=equal, equal_or_approx=equal_or_approx, min=min, max=max)
    except _CheckIntegerEqualError as error:
        raise _CheckLengthEqualError(obj=obj, equal=error.equal) from None
    except _CheckIntegerEqualOrApproxError as error:
        raise _CheckLengthEqualOrApproxError(
            obj=obj, equal_or_approx=error.equal_or_approx
        ) from None
    except _CheckIntegerMinError as error:
        raise _CheckLengthMinError(obj=obj, min_=error.min_) from None
    except _CheckIntegerMaxError as error:
        raise _CheckLengthMaxError(obj=obj, max_=error.max_) from None


@dataclass(frozen=True, kw_only=True, slots=True)
class CheckLengthError(Exception):
    obj: Sized


@dataclass(frozen=True, kw_only=True, slots=True)
class _CheckLengthEqualError(CheckLengthError):
    equal: int

    @override
    def __str__(self) -> str:
        return "Object {} must have length {}; got {}.".format(
            self.obj, self.equal, len(self.obj)
        )


@dataclass(frozen=True, kw_only=True, slots=True)
class _CheckLengthEqualOrApproxError(CheckLengthError):
    equal_or_approx: int | tuple[int, float]

    @override
    def __str__(self) -> str:
        match self.equal_or_approx:
            case target, error:
                desc = "approximate length {} (error {:%})".format(target, error)
            case target:
                desc = "length {}".format(target)
        return "Object {} must have {}; got {}.".format(self.obj, desc, len(self.obj))


@dataclass(frozen=True, kw_only=True, slots=True)
class _CheckLengthMinError(CheckLengthError):
    min_: int

    @override
    def __str__(self) -> str:
        return "Object {} must have minimum length {}; got {}.".format(
            self.obj, self.min_, len(self.obj)
        )


@dataclass(frozen=True, kw_only=True, slots=True)
class _CheckLengthMaxError(CheckLengthError):
    max_: int

    @override
    def __str__(self) -> str:
        return "Object {} must have maximum length {}; got {}.".format(
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
            "Sized objects {} and {} must have the same length; got {} and {}.".format(
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
class CheckMappingsEqualError(Exception, Generic[_K, _V]):
    left: Mapping[_K, _V]
    right: Mapping[_K, _V]
    left_extra: AbstractSet[_K]
    right_extra: AbstractSet[_K]
    errors: list[tuple[_K, _V, _V]]

    @override
    def __str__(self) -> str:
        match list(self._yield_parts()):
            case (desc,):
                pass
            case first, second:
                desc = "{} and {}".format(first, second)
            case first, second, third:
                desc = "{}, {} and {}".format(first, second, third)
            case _ as never:  # pragma: no cover
                assert_never(cast(Never, never))
        return "Mappings {} and {} must be equal; {}.".format(
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
            yield "differing values were {}".format(", ".join(error_descs))


def check_sets_equal(left: Iterable[Any], right: Iterable[Any], /) -> None:
    """Check that a pair of sets are equal."""
    left_as_set = set(left)
    right_as_set = set(right)
    left_extra = left_as_set - right_as_set
    right_extra = right_as_set - left_as_set
    if (len(left_extra) >= 1) or (len(right_extra) >= 1):
        raise CheckSetsEqualError(
            left=left_as_set,
            right=right_as_set,
            left_extra=left_extra,
            right_extra=right_extra,
        )


@dataclass(frozen=True, kw_only=True, slots=True)
class CheckSetsEqualError(Exception, Generic[_T]):
    left: AbstractSet[_T]
    right: AbstractSet[_T]
    left_extra: AbstractSet[_T]
    right_extra: AbstractSet[_T]

    @override
    def __str__(self) -> str:
        match list(self._yield_parts()):
            case (desc,):
                pass
            case first, second:
                desc = "{} and {}".format(first, second)
            case _ as never:  # pragma: no cover
                assert_never(cast(Never, never))
        return "Sets {} and {} must be equal; {}.".format(self.left, self.right, desc)

    def _yield_parts(self) -> Iterator[str]:
        if len(self.left_extra) >= 1:
            yield "left had extra items {}".format(self.left_extra)
        if len(self.right_extra) >= 1:
            yield "right had extra items {}".format(self.right_extra)


def check_submapping(left: Mapping[Any, Any], right: Mapping[Any, Any], /) -> None:
    """Check that a mapping is a subset of another mapping."""
    left_keys, right_keys = set(left), set(right)
    try:
        check_subset(left_keys, right_keys)
    except CheckSubSetError as error:
        extra = set(error.extra)
    else:
        extra = set()
    errors: list[tuple[Any, Any, Any]] = []
    for key in left_keys & right_keys:
        lv, rv = left[key], right[key]
        if lv != rv:
            errors.append((key, lv, rv))
    if (len(extra) >= 1) or (len(errors) >= 1):
        raise CheckSubMappingError(left=left, right=right, extra=extra, errors=errors)


@dataclass(frozen=True, kw_only=True, slots=True)
class CheckSubMappingError(Exception, Generic[_K, _V]):
    left: Mapping[_K, _V]
    right: Mapping[_K, _V]
    extra: AbstractSet[_K]
    errors: list[tuple[_K, _V, _V]]

    @override
    def __str__(self) -> str:
        match list(self._yield_parts()):
            case (desc,):
                pass
            case first, second:
                desc = "{} and {}".format(first, second)
            case _ as never:  # pragma: no cover
                assert_never(cast(Never, never))
        return "Mapping {} must be a submapping of {}; {}.".format(
            self.left, self.right, desc
        )

    def _yield_parts(self) -> Iterator[str]:
        if len(self.extra) >= 1:
            yield "left had extra keys {}".format(self.extra)
        if len(self.errors) >= 1:
            error_descs = (
                "({}, {}, k={})".format(lv, rv, k) for k, lv, rv in self.errors
            )
            yield "differing values were {}".format(", ".join(error_descs))


def check_subset(left: Iterable[Any], right: Iterable[Any], /) -> None:
    """Check that a set is a subset of another set."""
    left_as_set = set(left)
    right_as_set = set(right)
    extra = left_as_set - right_as_set
    if len(extra) >= 1:
        raise CheckSubSetError(left=left_as_set, right=right_as_set, extra=extra)


@dataclass(frozen=True, kw_only=True, slots=True)
class CheckSubSetError(Exception, Generic[_T]):
    left: AbstractSet[_T]
    right: AbstractSet[_T]
    extra: AbstractSet[_T]

    @override
    def __str__(self) -> str:
        return "Set {} must be a subset of {}; left had extra items {}.".format(
            self.left, self.right, self.extra
        )


def check_supermapping(left: Mapping[Any, Any], right: Mapping[Any, Any], /) -> None:
    """Check that a mapping is a superset of another mapping."""
    left_keys, right_keys = set(left), set(right)
    try:
        check_superset(left_keys, right_keys)
    except CheckSuperSetError as error:
        extra = set(error.extra)
    else:
        extra = set()
    errors: list[tuple[Any, Any, Any]] = []
    for key in left_keys & right_keys:
        lv, rv = left[key], right[key]
        if lv != rv:
            errors.append((key, lv, rv))
    if (len(extra) >= 1) or (len(errors) >= 1):
        raise CheckSuperMappingError(left=left, right=right, extra=extra, errors=errors)


@dataclass(frozen=True, kw_only=True, slots=True)
class CheckSuperMappingError(Exception, Generic[_K, _V]):
    left: Mapping[_K, _V]
    right: Mapping[_K, _V]
    extra: AbstractSet[_K]
    errors: list[tuple[_K, _V, _V]]

    @override
    def __str__(self) -> str:
        match list(self._yield_parts()):
            case (desc,):
                pass
            case first, second:
                desc = "{} and {}".format(first, second)
            case _ as never:  # pragma: no cover
                assert_never(cast(Never, never))
        return "Mapping {} must be a supermapping of {}; {}.".format(
            self.left, self.right, desc
        )

    def _yield_parts(self) -> Iterator[str]:
        if len(self.extra) >= 1:
            yield "right had extra keys {}".format(self.extra)
        if len(self.errors) >= 1:
            error_descs = (
                "({}, {}, k={})".format(lv, rv, k) for k, lv, rv in self.errors
            )
            yield "differing values were {}".format(", ".join(error_descs))


def check_superset(left: Iterable[Any], right: Iterable[Any], /) -> None:
    """Check that a set is a superset of another set."""
    left_as_set = set(left)
    right_as_set = set(right)
    extra = right_as_set - left_as_set
    if len(extra) >= 1:
        raise CheckSuperSetError(left=left_as_set, right=right_as_set, extra=extra)


@dataclass(frozen=True, kw_only=True, slots=True)
class CheckSuperSetError(Exception, Generic[_T]):
    left: AbstractSet[_T]
    right: AbstractSet[_T]
    extra: AbstractSet[_T]

    @override
    def __str__(self) -> str:
        return "Set {} must be a superset of {}; right had extra items {}.".format(
            self.left, self.right, self.extra
        )


def ensure_hashables(
    *args: Any, **kwargs: Any
) -> tuple[list[Hashable], dict[str, Hashable]]:
    """Ensure a set of positional & keyword arguments are all hashable."""
    hash_args = list(map(ensure_hashable, args))
    hash_kwargs = {k: ensure_hashable(v) for k, v in kwargs.items()}
    return hash_args, hash_kwargs


def ensure_iterable(obj: Any, /) -> Iterable[Any]:
    """Ensure an object is iterable."""
    if is_iterable(obj):
        return obj
    raise EnsureIterableError(obj=obj)


@dataclass(frozen=True, kw_only=True, slots=True)
class EnsureIterableError(Exception):
    obj: Any

    @override
    def __str__(self) -> str:
        return "Object {} must be iterable.".format(self.obj)


def ensure_iterable_not_str(obj: Any, /) -> Iterable[Any]:
    """Ensure an object is iterable, but not a string."""
    if is_iterable_not_str(obj):
        return obj
    raise EnsureIterableNotStrError(obj=obj)


@dataclass(frozen=True, kw_only=True, slots=True)
class EnsureIterableNotStrError(Exception):
    obj: Any

    @override
    def __str__(self) -> str:
        return "Object {} must be iterable, but not a string.".format(self.obj)


def is_iterable(obj: Any, /) -> TypeGuard[Iterable[Any]]:
    """Check if an object is iterable."""
    try:
        iter(obj)
    except TypeError:
        return False
    return True


def is_iterable_not_str(obj: Any, /) -> TypeGuard[Iterable[Any]]:
    """Check if an object is iterable, but not a string."""
    return is_iterable(obj) and not isinstance(obj, str)


__all__ = [
    "CheckDuplicatesError",
    "CheckIterablesEqualError",
    "CheckLengthsEqualError",
    "CheckMappingsEqualError",
    "CheckSetsEqualError",
    "CheckSubMappingError",
    "CheckSubSetError",
    "CheckSuperMappingError",
    "CheckSuperSetError",
    "EnsureIterableError",
    "EnsureIterableNotStrError",
    "check_duplicates",
    "check_iterables_equal",
    "check_lengths_equal",
    "check_mappings_equal",
    "check_sets_equal",
    "check_submapping",
    "check_subset",
    "check_supermapping",
    "check_superset",
    "ensure_hashables",
    "ensure_iterable",
    "ensure_iterable_not_str",
    "is_iterable",
    "is_iterable_not_str",
]
