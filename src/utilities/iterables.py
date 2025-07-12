from __future__ import annotations

import builtins
from collections import Counter
from collections.abc import (
    Callable,
    Hashable,
    Iterable,
    Iterator,
    Mapping,
    Sequence,
    Sized,
)
from collections.abc import Set as AbstractSet
from contextlib import suppress
from dataclasses import dataclass
from enum import Enum
from functools import cmp_to_key, partial, reduce
from itertools import accumulate, chain, groupby, islice, pairwise, product
from math import isnan
from operator import add, itemgetter, or_
from typing import (
    TYPE_CHECKING,
    Any,
    Literal,
    TypeGuard,
    assert_never,
    cast,
    overload,
    override,
)

from utilities.errors import ImpossibleCaseError
from utilities.functions import ensure_hashable, ensure_str
from utilities.math import (
    _CheckIntegerEqualError,
    _CheckIntegerEqualOrApproxError,
    _CheckIntegerMaxError,
    _CheckIntegerMinError,
    check_integer,
)
from utilities.reprlib import get_repr
from utilities.sentinel import Sentinel, sentinel
from utilities.types import SupportsAdd, SupportsLT

if TYPE_CHECKING:
    from types import NoneType

    from utilities.types import MaybeIterable, MaybeIterableHashable, Sign, StrMapping


##


def always_iterable[T](obj: MaybeIterable[T], /) -> Iterable[T]:
    """Typed version of `always_iterable`."""
    obj = cast("Any", obj)
    if isinstance(obj, str | bytes):
        return cast("list[T]", [obj])
    try:
        return iter(cast("Iterable[T]", obj))
    except TypeError:
        return cast("list[T]", [obj])


##


def always_iterable_hashable[T](
    obj: MaybeIterable[T] | None, /
) -> MaybeIterableHashable[T] | None:
    """Ensure an object is always hashable."""
    return None if obj is None else tuple(always_iterable(obj))


##


def apply_bijection[T, U](
    func: Callable[[T], U], iterable: Iterable[T], /
) -> Mapping[T, U]:
    """Apply a function bijectively."""
    keys = list(iterable)
    try:
        check_duplicates(keys)
    except CheckDuplicatesError as error:
        raise _ApplyBijectionDuplicateKeysError(
            keys=keys, counts=error.counts
        ) from None
    values = list(map(func, keys))
    try:
        check_duplicates(values)
    except CheckDuplicatesError as error:
        raise _ApplyBijectionDuplicateValuesError(
            keys=keys, values=values, counts=error.counts
        ) from None
    return dict(zip(keys, values, strict=True))


@dataclass(kw_only=True, slots=True)
class ApplyBijectionError[T](Exception):
    keys: list[T]
    counts: Mapping[T, int]


@dataclass(kw_only=True, slots=True)
class _ApplyBijectionDuplicateKeysError[T](ApplyBijectionError[T]):
    @override
    def __str__(self) -> str:
        return f"Keys {get_repr(self.keys)} must not contain duplicates; got {get_repr(self.counts)}"


@dataclass(kw_only=True, slots=True)
class _ApplyBijectionDuplicateValuesError[T, U](ApplyBijectionError[T]):
    values: list[U]

    @override
    def __str__(self) -> str:
        return f"Values {get_repr(self.values)} must not contain duplicates; got {get_repr(self.counts)}"


##


def apply_to_tuple[T](func: Callable[..., T], args: tuple[Any, ...], /) -> T:
    """Apply a function to a tuple of args."""
    return apply_to_varargs(func, *args)


##


def apply_to_varargs[T](func: Callable[..., T], *args: Any) -> T:
    """Apply a function to a variable number of arguments."""
    return func(*args)


##


@overload
def chain_mappings[K, V](
    *mappings: Mapping[K, V], list: Literal[True]
) -> Mapping[K, Sequence[V]]: ...
@overload
def chain_mappings[K, V](
    *mappings: Mapping[K, V], list: bool = False
) -> Mapping[K, Iterable[V]]: ...
def chain_mappings[K, V](
    *mappings: Mapping[K, V],
    list: bool = False,  # noqa: A002
) -> Mapping[K, Iterable[V]]:
    """Chain the values of a set of mappings."""
    try:
        first, *rest = mappings
    except ValueError:
        return {}
    initial = {k: [v] for k, v in first.items()}
    reduced = reduce(_chain_mappings_one, rest, initial)
    if list:
        return {k: builtins.list(v) for k, v in reduced.items()}
    return reduced


def _chain_mappings_one[K, V](
    acc: Mapping[K, Iterable[V]], el: Mapping[K, V], /
) -> Mapping[K, Iterable[V]]:
    """Chain the values of a set of mappings."""
    out = dict(acc)
    for key, value in el.items():
        out[key] = chain(out.get(key, []), [value])
    return out


##


def chain_maybe_iterables[T](*maybe_iterables: MaybeIterable[T]) -> Iterable[T]:
    """Chain a set of maybe iterables."""
    iterables = map(always_iterable, maybe_iterables)
    return chain.from_iterable(iterables)


##


def chain_nullable[T](*maybe_iterables: Iterable[T | None] | None) -> Iterable[T]:
    """Chain a set of values; ignoring nulls."""
    iterables = (mi for mi in maybe_iterables if mi is not None)
    values = ((i for i in it if i is not None) for it in iterables)
    return chain.from_iterable(values)


##


def check_bijection(mapping: Mapping[Any, Hashable], /) -> None:
    """Check if a mapping is a bijection."""
    try:
        check_duplicates(mapping.values())
    except CheckDuplicatesError as error:
        raise CheckBijectionError(mapping=mapping, counts=error.counts) from None


@dataclass(kw_only=True, slots=True)
class CheckBijectionError[THashable](Exception):
    mapping: Mapping[Any, THashable]
    counts: Mapping[THashable, int]

    @override
    def __str__(self) -> str:
        return f"Mapping {get_repr(self.mapping)} must be a bijection; got duplicates {get_repr(self.counts)}"


##


def check_duplicates(iterable: Iterable[Hashable], /) -> None:
    """Check if an iterable contains any duplicates."""
    counts = {k: v for k, v in Counter(iterable).items() if v > 1}
    if len(counts) >= 1:
        raise CheckDuplicatesError(iterable=iterable, counts=counts)


@dataclass(kw_only=True, slots=True)
class CheckDuplicatesError[THashable](Exception):
    iterable: Iterable[THashable]
    counts: Mapping[THashable, int]

    @override
    def __str__(self) -> str:
        return f"Iterable {get_repr(self.iterable)} must not contain duplicates; got {get_repr(self.counts)}"


##


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
                state = "right_longer"
            case "zip() argument 2 is shorter than argument 1":
                state = "left_longer"
            case _:  # pragma: no cover
                raise
    else:
        state = None
    if (len(errors) >= 1) or (state is not None):
        raise CheckIterablesEqualError(
            left=left_list, right=right_list, errors=errors, state=state
        )


type _CheckIterablesEqualState = Literal["left_longer", "right_longer"]


@dataclass(kw_only=True, slots=True)
class CheckIterablesEqualError[T](Exception):
    left: list[T]
    right: list[T]
    errors: list[tuple[int, T, T]]
    state: _CheckIterablesEqualState | None

    @override
    def __str__(self) -> str:
        parts = list(self._yield_parts())
        match parts:
            case (desc,):
                pass
            case first, second:
                desc = f"{first} and {second}"
            case _:  # pragma: no cover
                raise ImpossibleCaseError(case=[f"{parts=}"])
        return f"Iterables {get_repr(self.left)} and {get_repr(self.right)} must be equal; {desc}"

    def _yield_parts(self) -> Iterator[str]:
        if len(self.errors) >= 1:
            errors = [(f"{i=}", lv, rv) for i, lv, rv in self.errors]
            yield f"differing items were {get_repr(errors)}"
        match self.state:
            case "left_longer":
                yield "left was longer"
            case "right_longer":
                yield "right was longer"
            case None:
                pass
            case _ as never:
                assert_never(never)


##


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


@dataclass(kw_only=True, slots=True)
class CheckLengthError(Exception):
    obj: Sized


@dataclass(kw_only=True, slots=True)
class _CheckLengthEqualError(CheckLengthError):
    equal: int

    @override
    def __str__(self) -> str:
        return f"Object {get_repr(self.obj)} must have length {self.equal}; got {len(self.obj)}"


@dataclass(kw_only=True, slots=True)
class _CheckLengthEqualOrApproxError(CheckLengthError):
    equal_or_approx: int | tuple[int, float]

    @override
    def __str__(self) -> str:
        match self.equal_or_approx:
            case target, error:
                desc = f"approximate length {target} (error {error:%})"
            case target:
                desc = f"length {target}"
        return f"Object {get_repr(self.obj)} must have {desc}; got {len(self.obj)}"


@dataclass(kw_only=True, slots=True)
class _CheckLengthMinError(CheckLengthError):
    min_: int

    @override
    def __str__(self) -> str:
        return f"Object {get_repr(self.obj)} must have minimum length {self.min_}; got {len(self.obj)}"


@dataclass(kw_only=True, slots=True)
class _CheckLengthMaxError(CheckLengthError):
    max_: int

    @override
    def __str__(self) -> str:
        return f"Object {get_repr(self.obj)} must have maximum length {self.max_}; got {len(self.obj)}"


##


def check_lengths_equal(left: Sized, right: Sized, /) -> None:
    """Check that a pair of sizes objects have equal length."""
    if len(left) != len(right):
        raise CheckLengthsEqualError(left=left, right=right)


@dataclass(kw_only=True, slots=True)
class CheckLengthsEqualError(Exception):
    left: Sized
    right: Sized

    @override
    def __str__(self) -> str:
        return f"Sized objects {get_repr(self.left)} and {get_repr(self.right)} must have the same length; got {len(self.left)} and {len(self.right)}"


##


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


@dataclass(kw_only=True, slots=True)
class CheckMappingsEqualError[K, V](Exception):
    left: Mapping[K, V]
    right: Mapping[K, V]
    left_extra: AbstractSet[K]
    right_extra: AbstractSet[K]
    errors: list[tuple[K, V, V]]

    @override
    def __str__(self) -> str:
        parts = list(self._yield_parts())
        match parts:
            case (desc,):
                pass
            case first, second:
                desc = f"{first} and {second}"
            case first, second, third:
                desc = f"{first}, {second} and {third}"
            case _:  # pragma: no cover
                raise ImpossibleCaseError(case=[f"{parts=}"])
        return f"Mappings {get_repr(self.left)} and {get_repr(self.right)} must be equal; {desc}"

    def _yield_parts(self) -> Iterator[str]:
        if len(self.left_extra) >= 1:
            yield f"left had extra keys {get_repr(self.left_extra)}"
        if len(self.right_extra) >= 1:
            yield f"right had extra keys {get_repr(self.right_extra)}"
        if len(self.errors) >= 1:
            errors = [(f"{k=}", lv, rv) for k, lv, rv in self.errors]
            yield f"differing values were {get_repr(errors)}"


##


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


@dataclass(kw_only=True, slots=True)
class CheckSetsEqualError[T](Exception):
    left: AbstractSet[T]
    right: AbstractSet[T]
    left_extra: AbstractSet[T]
    right_extra: AbstractSet[T]

    @override
    def __str__(self) -> str:
        parts = list(self._yield_parts())
        match parts:
            case (desc,):
                pass
            case first, second:
                desc = f"{first} and {second}"
            case _:  # pragma: no cover
                raise ImpossibleCaseError(case=[f"{parts=}"])
        return f"Sets {get_repr(self.left)} and {get_repr(self.right)} must be equal; {desc}"

    def _yield_parts(self) -> Iterator[str]:
        if len(self.left_extra) >= 1:
            yield f"left had extra items {get_repr(self.left_extra)}"
        if len(self.right_extra) >= 1:
            yield f"right had extra items {get_repr(self.right_extra)}"


##


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


@dataclass(kw_only=True, slots=True)
class CheckSubMappingError[K, V](Exception):
    left: Mapping[K, V]
    right: Mapping[K, V]
    extra: AbstractSet[K]
    errors: list[tuple[K, V, V]]

    @override
    def __str__(self) -> str:
        parts = list(self._yield_parts())
        match parts:
            case (desc,):
                pass
            case first, second:
                desc = f"{first} and {second}"
            case _:  # pragma: no cover
                raise ImpossibleCaseError(case=[f"{parts=}"])
        return f"Mapping {get_repr(self.left)} must be a submapping of {get_repr(self.right)}; {desc}"

    def _yield_parts(self) -> Iterator[str]:
        if len(self.extra) >= 1:
            yield f"left had extra keys {get_repr(self.extra)}"
        if len(self.errors) >= 1:
            errors = [(f"{k=}", lv, rv) for k, lv, rv in self.errors]
            yield f"differing values were {get_repr(errors)}"


##


def check_subset(left: Iterable[Any], right: Iterable[Any], /) -> None:
    """Check that a set is a subset of another set."""
    left_as_set = set(left)
    right_as_set = set(right)
    extra = left_as_set - right_as_set
    if len(extra) >= 1:
        raise CheckSubSetError(left=left_as_set, right=right_as_set, extra=extra)


@dataclass(kw_only=True, slots=True)
class CheckSubSetError[T](Exception):
    left: AbstractSet[T]
    right: AbstractSet[T]
    extra: AbstractSet[T]

    @override
    def __str__(self) -> str:
        return f"Set {get_repr(self.left)} must be a subset of {get_repr(self.right)}; left had extra items {get_repr(self.extra)}"


##


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


@dataclass(kw_only=True, slots=True)
class CheckSuperMappingError[K, V](Exception):
    left: Mapping[K, V]
    right: Mapping[K, V]
    extra: AbstractSet[K]
    errors: list[tuple[K, V, V]]

    @override
    def __str__(self) -> str:
        parts = list(self._yield_parts())
        match parts:
            case (desc,):
                pass
            case first, second:
                desc = f"{first} and {second}"
            case _:  # pragma: no cover
                raise ImpossibleCaseError(case=[f"{parts=}"])
        return f"Mapping {get_repr(self.left)} must be a supermapping of {get_repr(self.right)}; {desc}"

    def _yield_parts(self) -> Iterator[str]:
        if len(self.extra) >= 1:
            yield f"right had extra keys {get_repr(self.extra)}"
        if len(self.errors) >= 1:
            errors = [(f"{k=}", lv, rv) for k, lv, rv in self.errors]
            yield f"differing values were {get_repr(errors)}"


##


def check_superset(left: Iterable[Any], right: Iterable[Any], /) -> None:
    """Check that a set is a superset of another set."""
    left_as_set = set(left)
    right_as_set = set(right)
    extra = right_as_set - left_as_set
    if len(extra) >= 1:
        raise CheckSuperSetError(left=left_as_set, right=right_as_set, extra=extra)


@dataclass(kw_only=True, slots=True)
class CheckSuperSetError[T](Exception):
    left: AbstractSet[T]
    right: AbstractSet[T]
    extra: AbstractSet[T]

    @override
    def __str__(self) -> str:
        return f"Set {get_repr(self.left)} must be a superset of {get_repr(self.right)}; right had extra items {get_repr(self.extra)}."


##


def check_unique_modulo_case(iterable: Iterable[str], /) -> None:
    """Check that an iterable of strings is unique modulo case."""
    try:
        _ = apply_bijection(str.lower, iterable)
    except _ApplyBijectionDuplicateKeysError as error:
        raise _CheckUniqueModuloCaseDuplicateStringsError(
            keys=error.keys, counts=error.counts
        ) from None
    except _ApplyBijectionDuplicateValuesError as error:
        raise _CheckUniqueModuloCaseDuplicateLowerCaseStringsError(
            keys=error.keys, values=error.values, counts=error.counts
        ) from None


@dataclass(kw_only=True, slots=True)
class CheckUniqueModuloCaseError(Exception):
    keys: Iterable[str]
    counts: Mapping[str, int]


@dataclass(kw_only=True, slots=True)
class _CheckUniqueModuloCaseDuplicateStringsError(CheckUniqueModuloCaseError):
    @override
    def __str__(self) -> str:
        return f"Strings {get_repr(self.keys)} must not contain duplicates; got {get_repr(self.counts)}"


@dataclass(kw_only=True, slots=True)
class _CheckUniqueModuloCaseDuplicateLowerCaseStringsError(CheckUniqueModuloCaseError):
    values: Iterable[str]

    @override
    def __str__(self) -> str:
        return f"Strings {get_repr(self.values)} must not contain duplicates (modulo case); got {get_repr(self.counts)}"


##


def cmp_nullable[T: SupportsLT](x: T | None, y: T | None, /) -> Sign:
    """Compare two nullable objects."""
    match x, y:
        case None, None:
            return 0
        case None, _:
            return -1
        case _, None:
            return 1
        case _, _:
            return cast("Sign", (x > y) - (x < y))
        case _ as never:
            assert_never(never)


##


def chunked[T](iterable: Iterable[T], n: int, /) -> Iterator[Sequence[T]]:
    """Break an iterable into lists of length n."""
    return iter(partial(take, n, iter(iterable)), [])


##


def ensure_hashables(
    *args: Any, **kwargs: Any
) -> tuple[list[Hashable], dict[str, Hashable]]:
    """Ensure a set of positional & keyword arguments are all hashable."""
    hash_args = list(map(ensure_hashable, args))
    hash_kwargs = {k: ensure_hashable(v) for k, v in kwargs.items()}
    return hash_args, hash_kwargs


##


def ensure_iterable(obj: Any, /) -> Iterable[Any]:
    """Ensure an object is iterable."""
    if is_iterable(obj):
        return obj
    raise EnsureIterableError(obj=obj)


@dataclass(kw_only=True, slots=True)
class EnsureIterableError(Exception):
    obj: Any

    @override
    def __str__(self) -> str:
        return f"Object {get_repr(self.obj)} must be iterable"


##


def ensure_iterable_not_str(obj: Any, /) -> Iterable[Any]:
    """Ensure an object is iterable, but not a string."""
    if is_iterable_not_str(obj):
        return obj
    raise EnsureIterableNotStrError(obj=obj)


@dataclass(kw_only=True, slots=True)
class EnsureIterableNotStrError(Exception):
    obj: Any

    @override
    def __str__(self) -> str:
        return f"Object {get_repr(self.obj)} must be iterable, but not a string"


##


_EDGE: int = 5


def enumerate_with_edge[T](
    iterable: Iterable[T], /, *, start: int = 0, edge: int = _EDGE
) -> Iterator[tuple[int, int, bool, T]]:
    """Enumerate an iterable, with the edge items marked."""
    as_list = list(iterable)
    total = len(as_list)
    indices = set(range(edge)) | set(range(total)[-edge:])
    is_edge = (i in indices for i in range(total))
    for (i, value), is_edge_i in zip(
        enumerate(as_list, start=start), is_edge, strict=True
    ):
        yield i, total, is_edge_i, value


##


def expanding_window[T](iterable: Iterable[T], /) -> islice[list[T]]:
    """Yield an expanding window over an iterable."""

    def func(acc: Iterable[T], el: T, /) -> list[T]:
        return list(chain(acc, [el]))

    return islice(accumulate(iterable, func=func, initial=[]), 1, None)


##


@overload
def filter_include_and_exclude[T, U](
    iterable: Iterable[T],
    /,
    *,
    include: MaybeIterable[U] | None = None,
    exclude: MaybeIterable[U] | None = None,
    key: Callable[[T], U],
) -> Iterable[T]: ...
@overload
def filter_include_and_exclude[T](
    iterable: Iterable[T],
    /,
    *,
    include: MaybeIterable[T] | None = None,
    exclude: MaybeIterable[T] | None = None,
    key: Callable[[T], Any] | None = None,
) -> Iterable[T]: ...
def filter_include_and_exclude[T, U](
    iterable: Iterable[T],
    /,
    *,
    include: MaybeIterable[U] | None = None,
    exclude: MaybeIterable[U] | None = None,
    key: Callable[[T], U] | None = None,
) -> Iterable[T]:
    """Filter an iterable based on an inclusion/exclusion pair."""
    include, exclude = resolve_include_and_exclude(include=include, exclude=exclude)
    if include is not None:
        if key is None:
            iterable = (x for x in iterable if x in include)
        else:
            iterable = (x for x in iterable if key(x) in include)
    if exclude is not None:
        if key is None:
            iterable = (x for x in iterable if x not in exclude)
        else:
            iterable = (x for x in iterable if key(x) not in exclude)
    return iterable


##


def group_consecutive_integers(iterable: Iterable[int], /) -> Iterable[tuple[int, int]]:
    """Group consecutive integers."""
    integers = sorted(iterable)
    for _, group in groupby(enumerate(integers), key=lambda x: x[1] - x[0]):
        as_list = list(map(itemgetter(1), group))
        yield as_list[0], as_list[-1]


def ungroup_consecutive_integers(
    iterable: Iterable[tuple[int, int]], /
) -> Iterable[int]:
    """Ungroup consecutive integers."""
    return chain.from_iterable(range(start, end + 1) for start, end in iterable)


##


@overload
def groupby_lists[T](
    iterable: Iterable[T], /, *, key: None = None
) -> Iterator[tuple[T, list[T]]]: ...
@overload
def groupby_lists[T, U](
    iterable: Iterable[T], /, *, key: Callable[[T], U]
) -> Iterator[tuple[U, list[T]]]: ...
def groupby_lists[T, U](
    iterable: Iterable[T], /, *, key: Callable[[T], U] | None = None
) -> Iterator[tuple[T, list[T]]] | Iterator[tuple[U, list[T]]]:
    """Yield consecutive keys and groups (as lists)."""
    if key is None:
        for k, group in groupby(iterable):
            yield k, list(group)
    else:
        for k, group in groupby(iterable, key=key):
            yield k, list(group)


##


def hashable_to_iterable[T: Hashable](obj: T | None, /) -> tuple[T, ...] | None:
    """Lift a hashable singleton to an iterable of hashables."""
    return None if obj is None else (obj,)


##


def is_iterable(obj: Any, /) -> TypeGuard[Iterable[Any]]:
    """Check if an object is iterable."""
    try:
        iter(obj)
    except TypeError:
        return False
    return True


##


def is_iterable_not_enum(obj: Any, /) -> TypeGuard[Iterable[Any]]:
    """Check if an object is iterable, but not an Enum."""
    return is_iterable(obj) and not (isinstance(obj, type) and issubclass(obj, Enum))


##


def is_iterable_not_str(obj: Any, /) -> TypeGuard[Iterable[Any]]:
    """Check if an object is iterable, but not a string."""
    return is_iterable(obj) and not isinstance(obj, str)


##


def map_mapping[K, V, W](
    func: Callable[[V], W], mapping: Mapping[K, V], /
) -> Mapping[K, W]:
    """Map a function over the values of a mapping."""
    return {k: func(v) for k, v in mapping.items()}


##


def merge_mappings[K, V](*mappings: Mapping[K, V]) -> Mapping[K, V]:
    """Merge a set of mappings."""
    return reduce(or_, map(dict, mappings), {})


##


def merge_sets[T](*iterables: Iterable[T]) -> AbstractSet[T]:
    """Merge a set of sets."""
    return reduce(or_, map(set, iterables), set())


##


def merge_str_mappings(
    *mappings: StrMapping, case_sensitive: bool = False
) -> StrMapping:
    """Merge a set of string mappings."""
    if case_sensitive:
        return merge_mappings(*mappings)
    return reduce(_merge_str_mappings_one, mappings, {})


def _merge_str_mappings_one(acc: StrMapping, el: StrMapping, /) -> StrMapping:
    out = dict(acc)
    try:
        check_unique_modulo_case(el)
    except _CheckUniqueModuloCaseDuplicateLowerCaseStringsError as error:
        raise MergeStrMappingsError(mapping=el, counts=error.counts) from None
    for key_add, value in el.items():
        try:
            key_del = one_str(out, key_add)
        except OneStrEmptyError:
            pass
        else:
            del out[key_del]
        out[key_add] = value
    return out


@dataclass(kw_only=True, slots=True)
class MergeStrMappingsError(Exception):
    mapping: StrMapping
    counts: Mapping[str, int]

    @override
    def __str__(self) -> str:
        return f"Mapping {get_repr(self.mapping)} keys must not contain duplicates (modulo case); got {get_repr(self.counts)}"


##


def one[T](*iterables: Iterable[T]) -> T:
    """Return the unique value in a set of iterables."""
    it = iter(chain(*iterables))
    try:
        first = next(it)
    except StopIteration:
        raise OneEmptyError(iterables=iterables) from None
    try:
        second = next(it)
    except StopIteration:
        return first
    raise OneNonUniqueError(iterables=iterables, first=first, second=second)


@dataclass(kw_only=True, slots=True)
class OneError[T](Exception):
    iterables: tuple[Iterable[T], ...]


@dataclass(kw_only=True, slots=True)
class OneEmptyError[T](OneError[T]):
    @override
    def __str__(self) -> str:
        return f"Iterable(s) {get_repr(self.iterables)} must not be empty"


@dataclass(kw_only=True, slots=True)
class OneNonUniqueError[T](OneError):
    first: T
    second: T

    @override
    def __str__(self) -> str:
        return f"Iterable(s) {get_repr(self.iterables)} must contain exactly one item; got {self.first}, {self.second} and perhaps more"


##


def one_maybe[T](*objs: MaybeIterable[T]) -> T:
    """Return the unique value in a set of values/iterables."""
    try:
        return one(chain_maybe_iterables(*objs))
    except OneEmptyError:
        raise OneMaybeEmptyError from None
    except OneNonUniqueError as error:
        raise OneMaybeNonUniqueError(
            objs=objs, first=error.first, second=error.second
        ) from None


@dataclass(kw_only=True, slots=True)
class OneMaybeError(Exception): ...


@dataclass(kw_only=True, slots=True)
class OneMaybeEmptyError(OneMaybeError):
    @override
    def __str__(self) -> str:
        return "Object(s) must not be empty"


@dataclass(kw_only=True, slots=True)
class OneMaybeNonUniqueError[T](OneMaybeError):
    objs: tuple[MaybeIterable[T], ...]
    first: T
    second: T

    @override
    def __str__(self) -> str:
        return f"Object(s) {get_repr(self.objs)} must contain exactly one item; got {self.first}, {self.second} and perhaps more"


##


def one_str(
    iterable: Iterable[str],
    text: str,
    /,
    *,
    head: bool = False,
    case_sensitive: bool = False,
) -> str:
    """Find the unique string in an iterable."""
    as_list = list(iterable)
    match head, case_sensitive:
        case False, True:
            it = (t for t in as_list if t == text)
        case False, False:
            it = (t for t in as_list if t.lower() == text.lower())
        case True, True:
            it = (t for t in as_list if t.startswith(text))
        case True, False:
            it = (t for t in as_list if t.lower().startswith(text.lower()))
        case _ as never:
            assert_never(never)
    try:
        return one(it)
    except OneEmptyError:
        raise OneStrEmptyError(
            iterable=as_list, text=text, head=head, case_sensitive=case_sensitive
        ) from None
    except OneNonUniqueError as error:
        raise OneStrNonUniqueError(
            iterable=as_list,
            text=text,
            head=head,
            case_sensitive=case_sensitive,
            first=error.first,
            second=error.second,
        ) from None


@dataclass(kw_only=True, slots=True)
class OneStrError(Exception):
    iterable: Iterable[str]
    text: str
    head: bool = False
    case_sensitive: bool = False


@dataclass(kw_only=True, slots=True)
class OneStrEmptyError(OneStrError):
    @override
    def __str__(self) -> str:
        head = f"Iterable {get_repr(self.iterable)} does not contain"
        match self.head, self.case_sensitive:
            case False, True:
                tail = repr(self.text)
            case False, False:
                tail = f"{self.text!r} (modulo case)"
            case True, True:
                tail = f"any string starting with {self.text!r}"
            case True, False:
                tail = f"any string starting with {self.text!r} (modulo case)"
            case _ as never:
                assert_never(never)
        return f"{head} {tail}"


@dataclass(kw_only=True, slots=True)
class OneStrNonUniqueError(OneStrError):
    first: str
    second: str

    @override
    def __str__(self) -> str:
        head = f"Iterable {get_repr(self.iterable)} must contain"
        match self.head, self.case_sensitive:
            case False, True:
                mid = f"{self.text!r} exactly once"
            case False, False:
                mid = f"{self.text!r} exactly once (modulo case)"
            case True, True:
                mid = f"exactly one string starting with {self.text!r}"
            case True, False:
                mid = f"exactly one string starting with {self.text!r} (modulo case)"
            case _ as never:
                assert_never(never)
        return f"{head} {mid}; got {self.first!r}, {self.second!r} and perhaps more"


##


def one_unique[T: Hashable](*iterables: Iterable[T]) -> T:
    """Return the set-unique value in a set of iterables."""
    try:
        return one(set(chain(*iterables)))
    except OneEmptyError:
        raise OneUniqueEmptyError from None
    except OneNonUniqueError as error:
        raise OneUniqueNonUniqueError(
            iterables=iterables, first=error.first, second=error.second
        ) from None


@dataclass(kw_only=True, slots=True)
class OneUniqueError(Exception): ...


@dataclass(kw_only=True, slots=True)
class OneUniqueEmptyError(OneUniqueError):
    @override
    def __str__(self) -> str:
        return "Iterable(s) must not be empty"


@dataclass(kw_only=True, slots=True)
class OneUniqueNonUniqueError[THashable](OneUniqueError):
    iterables: tuple[MaybeIterable[THashable], ...]
    first: THashable
    second: THashable

    @override
    def __str__(self) -> str:
        return f"Iterable(s) {get_repr(self.iterables)} must contain exactly one item; got {self.first}, {self.second} and perhaps more"


##


def pairwise_tail[T](iterable: Iterable[T], /) -> Iterator[tuple[T, T | Sentinel]]:
    """Return pairwise elements, with the last paired with the sentinel."""
    return pairwise(chain(iterable, [sentinel]))


##


def product_dicts[K, V](mapping: Mapping[K, Iterable[V]], /) -> Iterator[Mapping[K, V]]:
    """Return the cartesian product of the values in a mapping, as mappings."""
    keys = list(mapping)
    for values in product(*mapping.values()):
        yield cast("Mapping[K, V]", dict(zip(keys, values, strict=True)))


##


def range_partitions(stop: int, num: int, total: int, /) -> range:
    """Partition a range."""
    if stop <= 0:
        raise _RangePartitionsStopError(stop=stop)
    if not (1 <= total <= stop):
        raise _RangePartitionsTotalError(stop=stop, total=total)
    if not (0 <= num < total):
        raise _RangePartitionsNumError(num=num, total=total)
    q, r = divmod(stop, total)
    start = num * q + min(num, r)
    end = start + q + (1 if num < r else 0)
    return range(start, end)


@dataclass(kw_only=True, slots=True)
class RangePartitionsError(Exception): ...


@dataclass(kw_only=True, slots=True)
class _RangePartitionsStopError(RangePartitionsError):
    stop: int

    @override
    def __str__(self) -> str:
        return f"'stop' must be positive; got {self.stop}"


@dataclass(kw_only=True, slots=True)
class _RangePartitionsTotalError(RangePartitionsError):
    stop: int
    total: int

    @override
    def __str__(self) -> str:
        return f"'total' must be in [1, {self.stop}]; got {self.total}"


@dataclass(kw_only=True, slots=True)
class _RangePartitionsNumError(RangePartitionsError):
    num: int
    total: int

    @override
    def __str__(self) -> str:
        return f"'num' must be in [0, {self.total - 1}]; got {self.num}"


##


@overload
def reduce_mappings[K, V](
    func: Callable[[V, V], V], sequence: Iterable[Mapping[K, V]], /
) -> Mapping[K, V]: ...
@overload
def reduce_mappings[K, V, W](
    func: Callable[[W, V], W],
    sequence: Iterable[Mapping[K, V]],
    /,
    *,
    initial: W | Sentinel = sentinel,
) -> Mapping[K, W]: ...
def reduce_mappings[K, V, W](
    func: Callable[[V, V], V] | Callable[[W, V], W],
    sequence: Iterable[Mapping[K, V]],
    /,
    *,
    initial: W | Sentinel = sentinel,
) -> Mapping[K, V | W]:
    """Reduce a function over the values of a set of mappings."""
    chained = chain_mappings(*sequence)
    if isinstance(initial, Sentinel):
        func2 = cast("Callable[[V, V], V]", func)
        return {k: reduce(func2, v) for k, v in chained.items()}
    func2 = cast("Callable[[W, V], W]", func)
    return {k: reduce(func2, v, initial) for k, v in chained.items()}


##


def resolve_include_and_exclude[T](
    *, include: MaybeIterable[T] | None = None, exclude: MaybeIterable[T] | None = None
) -> tuple[set[T] | None, set[T] | None]:
    """Resolve an inclusion/exclusion pair."""
    include_use = include if include is None else set(always_iterable(include))
    exclude_use = exclude if exclude is None else set(always_iterable(exclude))
    if (
        (include_use is not None)
        and (exclude_use is not None)
        and (len(include_use & exclude_use) >= 1)
    ):
        raise ResolveIncludeAndExcludeError(include=include_use, exclude=exclude_use)
    return include_use, exclude_use


@dataclass(kw_only=True, slots=True)
class ResolveIncludeAndExcludeError[T](Exception):
    include: Iterable[T]
    exclude: Iterable[T]

    @override
    def __str__(self) -> str:
        include = list(self.include)
        exclude = list(self.exclude)
        overlap = set(include) & set(exclude)
        return f"Iterables {get_repr(include)} and {get_repr(exclude)} must not overlap; got {get_repr(overlap)}"


##


def sort_iterable[T](iterable: Iterable[T], /) -> list[T]:
    """Sort an iterable across types."""
    return sorted(iterable, key=cmp_to_key(_sort_iterable_cmp))


def _sort_iterable_cmp(x: Any, y: Any, /) -> Sign:
    """Compare two quantities."""
    if type(x) is not type(y):
        x_qualname = type(x).__qualname__
        y_qualname = type(y).__qualname__
        if x_qualname < y_qualname:
            return -1
        if x_qualname > y_qualname:
            return 1
        raise ImpossibleCaseError(  # pragma: no cover
            case=[f"{x_qualname=}", f"{y_qualname=}"]
        )

    # singletons
    if x is None:
        y = cast("NoneType", y)
        return 0
    if isinstance(x, float):
        y = cast("float", y)
        return _sort_iterable_cmp_floats(x, y)
    if isinstance(x, str):  # else Sequence
        y = cast("str", y)
        return cast("Sign", (x > y) - (x < y))

    # collections
    if isinstance(x, Sized):
        y = cast("Sized", y)
        if (result := _sort_iterable_cmp(len(x), len(y))) != 0:
            return result
    if isinstance(x, Mapping):
        y = cast("Mapping[Any, Any]", y)
        return _sort_iterable_cmp(x.items(), y.items())
    if isinstance(x, AbstractSet):
        y = cast("AbstractSet[Any]", y)
        return _sort_iterable_cmp(sort_iterable(x), sort_iterable(y))
    if isinstance(x, Sequence):
        y = cast("Sequence[Any]", y)
        it: Iterable[Sign] = (
            _sort_iterable_cmp(x_i, y_i) for x_i, y_i in zip(x, y, strict=True)
        )
        with suppress(StopIteration):
            return next(r for r in it if r != 0)

    try:
        return cast("Sign", (x > y) - (x < y))
    except TypeError:
        raise SortIterableError(x=x, y=y) from None


@dataclass(kw_only=True, slots=True)
class SortIterableError(Exception):
    x: Any
    y: Any

    @override
    def __str__(self) -> str:
        return f"Unable to sort {get_repr(self.x)} and {get_repr(self.y)}"


def _sort_iterable_cmp_floats(x: float, y: float, /) -> Sign:
    """Compare two floats."""
    x_nan, y_nan = map(isnan, [x, y])
    match x_nan, y_nan:
        case True, True:
            return 0
        case True, False:
            return 1
        case False, True:
            return -1
        case False, False:
            return cast("Sign", (x > y) - (x < y))
        case _ as never:
            assert_never(never)


##


def sum_mappings[K: Hashable, V: SupportsAdd](
    *mappings: Mapping[K, V],
) -> Mapping[K, V]:
    """Sum the values of a set of mappings."""
    return reduce_mappings(add, mappings, initial=0)


##


def take[T](n: int, iterable: Iterable[T], /) -> Sequence[T]:
    """Return first n items of the iterable as a list."""
    return list(islice(iterable, n))


##


@overload
def transpose[T1](iterable: Iterable[tuple[T1]], /) -> tuple[list[T1]]: ...
@overload
def transpose[T1, T2](
    iterable: Iterable[tuple[T1, T2]], /
) -> tuple[list[T1], list[T2]]: ...
@overload
def transpose[T1, T2, T3](
    iterable: Iterable[tuple[T1, T2, T3]], /
) -> tuple[list[T1], list[T2], list[T3]]: ...
@overload
def transpose[T1, T2, T3, T4](
    iterable: Iterable[tuple[T1, T2, T3, T4]], /
) -> tuple[list[T1], list[T2], list[T3], list[T4]]: ...
@overload
def transpose[T1, T2, T3, T4, T5](
    iterable: Iterable[tuple[T1, T2, T3, T4, T5]], /
) -> tuple[list[T1], list[T2], list[T3], list[T4], list[T5]]: ...
def transpose(iterable: Iterable[tuple[Any]]) -> tuple[list[Any], ...]:  # pyright: ignore[reportInconsistentOverload]
    """Typed verison of `transpose`."""
    return tuple(map(list, zip(*iterable, strict=True)))


##


def unique_everseen[T](
    iterable: Iterable[T], /, *, key: Callable[[T], Any] | None = None
) -> Iterator[T]:
    """Yield unique elements, preserving order."""
    seenset = set()
    seenset_add = seenset.add
    seenlist = []
    seenlist_add = seenlist.append
    use_key = key is not None
    for element in iterable:
        k = key(element) if use_key else element
        try:
            if k not in seenset:
                seenset_add(k)
                yield element
        except TypeError:
            if k not in seenlist:
                seenlist_add(k)
                yield element


##


__all__ = [
    "ApplyBijectionError",
    "CheckBijectionError",
    "CheckDuplicatesError",
    "CheckIterablesEqualError",
    "CheckLengthsEqualError",
    "CheckMappingsEqualError",
    "CheckSetsEqualError",
    "CheckSubMappingError",
    "CheckSubSetError",
    "CheckSuperMappingError",
    "CheckSuperSetError",
    "CheckUniqueModuloCaseError",
    "EnsureIterableError",
    "EnsureIterableNotStrError",
    "MergeStrMappingsError",
    "OneEmptyError",
    "OneError",
    "OneMaybeEmptyError",
    "OneMaybeError",
    "OneMaybeNonUniqueError",
    "OneNonUniqueError",
    "OneStrEmptyError",
    "OneStrError",
    "OneStrNonUniqueError",
    "OneUniqueEmptyError",
    "OneUniqueError",
    "OneUniqueNonUniqueError",
    "RangePartitionsError",
    "ResolveIncludeAndExcludeError",
    "SortIterableError",
    "always_iterable",
    "always_iterable_hashable",
    "apply_bijection",
    "apply_to_tuple",
    "apply_to_varargs",
    "chain_mappings",
    "chain_maybe_iterables",
    "chain_nullable",
    "check_bijection",
    "check_duplicates",
    "check_iterables_equal",
    "check_lengths_equal",
    "check_mappings_equal",
    "check_sets_equal",
    "check_submapping",
    "check_subset",
    "check_supermapping",
    "check_superset",
    "check_unique_modulo_case",
    "chunked",
    "cmp_nullable",
    "ensure_hashables",
    "ensure_iterable",
    "ensure_iterable_not_str",
    "enumerate_with_edge",
    "expanding_window",
    "filter_include_and_exclude",
    "group_consecutive_integers",
    "groupby_lists",
    "hashable_to_iterable",
    "is_iterable",
    "is_iterable_not_enum",
    "is_iterable_not_str",
    "map_mapping",
    "merge_mappings",
    "merge_sets",
    "merge_str_mappings",
    "one",
    "one_maybe",
    "one_str",
    "one_unique",
    "pairwise_tail",
    "product_dicts",
    "range_partitions",
    "reduce_mappings",
    "resolve_include_and_exclude",
    "sort_iterable",
    "sum_mappings",
    "take",
    "transpose",
    "ungroup_consecutive_integers",
    "unique_everseen",
]
