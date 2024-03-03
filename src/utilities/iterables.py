from __future__ import annotations

from collections import Counter
from collections.abc import Hashable, Iterable, Iterator, Mapping, Sequence, Sized
from collections.abc import Set as AbstractSet
from dataclasses import dataclass
from enum import Enum, auto
from functools import partial
from itertools import islice
from typing import Any, Generic, TypeGuard, TypeVar, cast, overload

from typing_extensions import Never, assert_never, override

from utilities.math import (
    _CheckIntegerEqualError,
    _CheckIntegerEqualOrApproxError,
    _CheckIntegerMaxError,
    _CheckIntegerMinError,
    check_integer,
)
from utilities.text import ensure_str
from utilities.types import ensure_hashable

_K = TypeVar("_K")
_T = TypeVar("_T")
_V = TypeVar("_V")
_T1 = TypeVar("_T1")
_T2 = TypeVar("_T2")
_T3 = TypeVar("_T3")
_T4 = TypeVar("_T4")
_T5 = TypeVar("_T5")


def check_duplicates(iterable: Iterable[Hashable], /) -> None:
    """Check if an iterable contains any duplicates."""
    counts = {k: v for k, v in Counter(iterable).items() if v > 1}
    if len(counts) >= 1:
        raise CheckDuplicatesError(iterable=iterable, counts=counts)


@dataclass(kw_only=True)
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
                raise
    else:
        state = None
    if (len(errors) >= 1) or (state is not None):
        raise CheckIterablesEqualError(
            left=left_list, right=right_list, errors=errors, state=state
        )


@dataclass(kw_only=True)
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
                desc = f"{first} and {second}"
            case _ as never:  # pragma: no cover
                assert_never(cast(Never, never))
        return f"Iterables {self.left} and {self.right} must be equal; {desc}."

    def _yield_parts(self) -> Iterator[str]:
        if len(self.errors) >= 1:
            error_descs = (f"({lv}, {rv}, i={i})" for i, lv, rv in self.errors)
            yield "differing items were {}".format(", ".join(error_descs))
        match self.state:
            case _CheckIterablesEqualState.left_longer:
                yield "left was longer"
            case _CheckIterablesEqualState.right_longer:
                yield "right was longer"
            case None:
                pass
            case _ as never:  # type: ignore[]
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


@dataclass(kw_only=True)
class CheckLengthError(Exception):
    obj: Sized


@dataclass(kw_only=True)
class _CheckLengthEqualError(CheckLengthError):
    equal: int

    @override
    def __str__(self) -> str:
        return f"Object {self.obj} must have length {self.equal}; got {len(self.obj)}."


@dataclass(kw_only=True)
class _CheckLengthEqualOrApproxError(CheckLengthError):
    equal_or_approx: int | tuple[int, float]

    @override
    def __str__(self) -> str:
        match self.equal_or_approx:
            case target, error:
                desc = f"approximate length {target} (error {error:%})"
            case target:
                desc = f"length {target}"
        return f"Object {self.obj} must have {desc}; got {len(self.obj)}."


@dataclass(kw_only=True)
class _CheckLengthMinError(CheckLengthError):
    min_: int

    @override
    def __str__(self) -> str:
        return f"Object {self.obj} must have minimum length {self.min_}; got {len(self.obj)}."


@dataclass(kw_only=True)
class _CheckLengthMaxError(CheckLengthError):
    max_: int

    @override
    def __str__(self) -> str:
        return f"Object {self.obj} must have maximum length {self.max_}; got {len(self.obj)}."


def check_lengths_equal(left: Sized, right: Sized, /) -> None:
    """Check that a pair of sizes objects have equal length."""
    if len(left) != len(right):
        raise CheckLengthsEqualError(left=left, right=right)


@dataclass(kw_only=True)
class CheckLengthsEqualError(Exception):
    left: Sized
    right: Sized

    @override
    def __str__(self) -> str:
        return f"Sized objects {self.left} and {self.right} must have the same length; got {len(self.left)} and {len(self.right)}."


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


@dataclass(kw_only=True)
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
                desc = f"{first} and {second}"
            case first, second, third:
                desc = f"{first}, {second} and {third}"
            case _ as never:  # pragma: no cover
                assert_never(cast(Never, never))
        return f"Mappings {self.left} and {self.right} must be equal; {desc}."

    def _yield_parts(self) -> Iterator[str]:
        if len(self.left_extra) >= 1:
            yield f"left had extra keys {self.left_extra}"
        if len(self.right_extra) >= 1:
            yield f"right had extra keys {self.right_extra}"
        if len(self.errors) >= 1:
            error_descs = (f"({lv}, {rv}, k={k})" for k, lv, rv in self.errors)
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


@dataclass(kw_only=True)
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
                desc = f"{first} and {second}"
            case _ as never:  # pragma: no cover
                assert_never(cast(Never, never))
        return f"Sets {self.left} and {self.right} must be equal; {desc}."

    def _yield_parts(self) -> Iterator[str]:
        if len(self.left_extra) >= 1:
            yield f"left had extra items {self.left_extra}"
        if len(self.right_extra) >= 1:
            yield f"right had extra items {self.right_extra}"


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


@dataclass(kw_only=True)
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
                desc = f"{first} and {second}"
            case _ as never:  # pragma: no cover
                assert_never(cast(Never, never))
        return f"Mapping {self.left} must be a submapping of {self.right}; {desc}."

    def _yield_parts(self) -> Iterator[str]:
        if len(self.extra) >= 1:
            yield f"left had extra keys {self.extra}"
        if len(self.errors) >= 1:
            error_descs = (f"({lv}, {rv}, k={k})" for k, lv, rv in self.errors)
            yield "differing values were {}".format(", ".join(error_descs))


def check_subset(left: Iterable[Any], right: Iterable[Any], /) -> None:
    """Check that a set is a subset of another set."""
    left_as_set = set(left)
    right_as_set = set(right)
    extra = left_as_set - right_as_set
    if len(extra) >= 1:
        raise CheckSubSetError(left=left_as_set, right=right_as_set, extra=extra)


@dataclass(kw_only=True)
class CheckSubSetError(Exception, Generic[_T]):
    left: AbstractSet[_T]
    right: AbstractSet[_T]
    extra: AbstractSet[_T]

    @override
    def __str__(self) -> str:
        return f"Set {self.left} must be a subset of {self.right}; left had extra items {self.extra}."


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


@dataclass(kw_only=True)
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
                desc = f"{first} and {second}"
            case _ as never:  # pragma: no cover
                assert_never(cast(Never, never))
        return f"Mapping {self.left} must be a supermapping of {self.right}; {desc}."

    def _yield_parts(self) -> Iterator[str]:
        if len(self.extra) >= 1:
            yield f"right had extra keys {self.extra}"
        if len(self.errors) >= 1:
            error_descs = (f"({lv}, {rv}, k={k})" for k, lv, rv in self.errors)
            yield "differing values were {}".format(", ".join(error_descs))


def check_superset(left: Iterable[Any], right: Iterable[Any], /) -> None:
    """Check that a set is a superset of another set."""
    left_as_set = set(left)
    right_as_set = set(right)
    extra = right_as_set - left_as_set
    if len(extra) >= 1:
        raise CheckSuperSetError(left=left_as_set, right=right_as_set, extra=extra)


@dataclass(kw_only=True)
class CheckSuperSetError(Exception, Generic[_T]):
    left: AbstractSet[_T]
    right: AbstractSet[_T]
    extra: AbstractSet[_T]

    @override
    def __str__(self) -> str:
        return f"Set {self.left} must be a superset of {self.right}; right had extra items {self.extra}."


def chunked(iterable: Iterable[_T], n: int, /) -> Iterable[Sequence[_T]]:
    """Break an iterable into lists of length n."""
    return cast(Iterable[Sequence[_T]], iter(partial(take, n, iter(iterable)), []))


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


@dataclass(kw_only=True)
class EnsureIterableError(Exception):
    obj: Any

    @override
    def __str__(self) -> str:
        return f"Object {self.obj} must be iterable."


def ensure_iterable_not_str(obj: Any, /) -> Iterable[Any]:
    """Ensure an object is iterable, but not a string."""
    if is_iterable_not_str(obj):
        return obj
    raise EnsureIterableNotStrError(obj=obj)


@dataclass(kw_only=True)
class EnsureIterableNotStrError(Exception):
    obj: Any

    @override
    def __str__(self) -> str:
        return f"Object {self.obj} must be iterable, but not a string."


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


@dataclass(kw_only=True)
class OneError(Exception, Generic[_T]):
    iterable: Iterable[_T]


@dataclass(kw_only=True)
class OneEmptyError(OneError[_T]):
    @override
    def __str__(self) -> str:
        return f"Iterable {self.iterable} must not be empty."


@dataclass(kw_only=True)
class OneNonUniqueError(OneError[_T]):
    first: _T
    second: _T

    @override
    def __str__(self) -> str:
        return f"Iterable {self.iterable} must contain exactly one item; got {self.first}, {self.second} and perhaps more."


def one(iterable: Iterable[_T], /) -> _T:
    """Custom version of `one` with separate exceptions."""
    it = iter(iterable)
    try:
        first = next(it)
    except StopIteration:
        raise OneEmptyError(iterable=iterable) from None
    try:
        second = next(it)
    except StopIteration:
        return first
    raise OneNonUniqueError(iterable=iterable, first=first, second=second)


def take(n: int, iterable: Iterable[_T], /) -> Sequence[_T]:
    """Return first n items of the iterable as a list."""
    return list(islice(iterable, n))


@overload
def transpose(iterable: Iterable[tuple[_T1]], /) -> tuple[tuple[_T1, ...]]: ...


@overload
def transpose(
    iterable: Iterable[tuple[_T1, _T2]], /
) -> tuple[tuple[_T1, ...], tuple[_T2, ...]]: ...


@overload
def transpose(
    iterable: Iterable[tuple[_T1, _T2, _T3]], /
) -> tuple[tuple[_T1, ...], tuple[_T2, ...], tuple[_T3, ...]]: ...


@overload
def transpose(
    iterable: Iterable[tuple[_T1, _T2, _T3, _T4]], /
) -> tuple[tuple[_T1, ...], tuple[_T2, ...], tuple[_T3, ...], tuple[_T4, ...]]: ...


@overload
def transpose(
    iterable: Iterable[tuple[_T1, _T2, _T3, _T4, _T5]], /
) -> tuple[
    tuple[_T1, ...], tuple[_T2, ...], tuple[_T3, ...], tuple[_T4, ...], tuple[_T5, ...]
]: ...


def transpose(iterable: Iterable[tuple[Any, ...]]) -> tuple[tuple[Any, ...], ...]:
    """Typed verison of `transpose`."""
    return tuple(zip(*iterable, strict=True))


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
    "OneEmptyError",
    "OneError",
    "OneNonUniqueError",
    "check_duplicates",
    "check_iterables_equal",
    "check_lengths_equal",
    "check_mappings_equal",
    "check_sets_equal",
    "check_submapping",
    "check_subset",
    "check_supermapping",
    "check_superset",
    "chunked",
    "ensure_hashables",
    "ensure_iterable",
    "ensure_iterable_not_str",
    "is_iterable",
    "is_iterable_not_str",
    "one",
    "take",
    "transpose",
]
