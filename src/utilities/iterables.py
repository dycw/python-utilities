from __future__ import annotations

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
from functools import cmp_to_key, reduce
from itertools import groupby
from math import isnan
from operator import or_
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

from rich.pretty import pretty_repr

from utilities.core import (
    CheckUniqueError,
    OneStrEmptyError,
    always_iterable,
    check_unique,
    one,
    one_str,
)
from utilities.errors import ImpossibleCaseError
from utilities.math import (
    _CheckIntegerEqualError,
    _CheckIntegerEqualOrApproxError,
    _CheckIntegerMaxError,
    _CheckIntegerMinError,
    check_integer,
)
from utilities.types import SupportsLT

if TYPE_CHECKING:
    from types import NoneType

    from utilities.types import MaybeIterable, Sign, StrMapping


def apply_bijection[T, U](
    func: Callable[[T], U], iterable: Iterable[T], /
) -> Mapping[T, U]:
    """Apply a function bijectively."""
    keys = list(iterable)
    try:
        check_unique(*keys)
    except CheckUniqueError as error:
        raise _ApplyBijectionDuplicateKeysError(
            keys=keys, counts=error.counts
        ) from None
    values = list(map(func, keys))
    try:
        check_unique(*values)
    except CheckUniqueError as error:
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
        return f"Keys {pretty_repr(self.keys)} must not contain duplicates; got {pretty_repr(self.counts)}"


@dataclass(kw_only=True, slots=True)
class _ApplyBijectionDuplicateValuesError[T, U](ApplyBijectionError[T]):
    values: list[U]

    @override
    def __str__(self) -> str:
        return f"Values {pretty_repr(self.values)} must not contain duplicates; got {pretty_repr(self.counts)}"


##


def apply_to_tuple[T](func: Callable[..., T], args: tuple[Any, ...], /) -> T:
    """Apply a function to a tuple of args."""
    return apply_to_varargs(func, *args)


def apply_to_varargs[T](func: Callable[..., T], *args: Any) -> T:
    """Apply a function to a variable number of arguments."""
    return func(*args)


##


def check_bijection(mapping: Mapping[Any, Hashable], /) -> None:
    """Check if a mapping is a bijection."""
    try:
        check_unique(*mapping.values())
    except CheckUniqueError as error:
        raise CheckBijectionError(mapping=mapping, counts=error.counts) from None


@dataclass(kw_only=True, slots=True)
class CheckBijectionError[THashable](Exception):
    mapping: Mapping[Any, THashable]
    counts: Mapping[THashable, int]

    @override
    def __str__(self) -> str:
        return f"Mapping {pretty_repr(self.mapping)} must be a bijection; got duplicates {pretty_repr(self.counts)}"


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
        match one(error.args):
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
                ...
            case first, second:
                desc = f"{first} and {second}"
            case _:  # pragma: no cover
                raise ImpossibleCaseError(case=[f"{parts=}"])
        return f"Iterables {pretty_repr(self.left)} and {pretty_repr(self.right)} must be equal; {desc}"

    def _yield_parts(self) -> Iterator[str]:
        if len(self.errors) >= 1:
            errors = [(f"{i=}", lv, rv) for i, lv, rv in self.errors]
            yield f"differing items were {pretty_repr(errors)}"
        match self.state:
            case "left_longer":
                yield "left was longer"
            case "right_longer":
                yield "right was longer"
            case None:
                ...
            case never:
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
        return f"Object {pretty_repr(self.obj)} must have length {self.equal}; got {len(self.obj)}"


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
        return f"Object {pretty_repr(self.obj)} must have {desc}; got {len(self.obj)}"


@dataclass(kw_only=True, slots=True)
class _CheckLengthMinError(CheckLengthError):
    min_: int

    @override
    def __str__(self) -> str:
        return f"Object {pretty_repr(self.obj)} must have minimum length {self.min_}; got {len(self.obj)}"


@dataclass(kw_only=True, slots=True)
class _CheckLengthMaxError(CheckLengthError):
    max_: int

    @override
    def __str__(self) -> str:
        return f"Object {pretty_repr(self.obj)} must have maximum length {self.max_}; got {len(self.obj)}"


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
        return f"Sized objects {pretty_repr(self.left)} and {pretty_repr(self.right)} must have the same length; got {len(self.left)} and {len(self.right)}"


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
                ...
            case first, second:
                desc = f"{first} and {second}"
            case first, second, third:
                desc = f"{first}, {second} and {third}"
            case _:  # pragma: no cover
                raise ImpossibleCaseError(case=[f"{parts=}"])
        return f"Mappings {pretty_repr(self.left)} and {pretty_repr(self.right)} must be equal; {desc}"

    def _yield_parts(self) -> Iterator[str]:
        if len(self.left_extra) >= 1:
            yield f"left had extra keys {pretty_repr(self.left_extra)}"
        if len(self.right_extra) >= 1:
            yield f"right had extra keys {pretty_repr(self.right_extra)}"
        if len(self.errors) >= 1:
            errors = [(f"{k=}", lv, rv) for k, lv, rv in self.errors]
            yield f"differing values were {pretty_repr(errors)}"


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
                ...
            case first, second:
                desc = f"{first} and {second}"
            case _:  # pragma: no cover
                raise ImpossibleCaseError(case=[f"{parts=}"])
        return f"Sets {pretty_repr(self.left)} and {pretty_repr(self.right)} must be equal; {desc}"

    def _yield_parts(self) -> Iterator[str]:
        if len(self.left_extra) >= 1:
            yield f"left had extra items {pretty_repr(self.left_extra)}"
        if len(self.right_extra) >= 1:
            yield f"right had extra items {pretty_repr(self.right_extra)}"


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
                ...
            case first, second:
                desc = f"{first} and {second}"
            case _:  # pragma: no cover
                raise ImpossibleCaseError(case=[f"{parts=}"])
        return f"Mapping {pretty_repr(self.left)} must be a submapping of {pretty_repr(self.right)}; {desc}"

    def _yield_parts(self) -> Iterator[str]:
        if len(self.extra) >= 1:
            yield f"left had extra keys {pretty_repr(self.extra)}"
        if len(self.errors) >= 1:
            errors = [(f"{k=}", lv, rv) for k, lv, rv in self.errors]
            yield f"differing values were {pretty_repr(errors)}"


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
        return f"Set {pretty_repr(self.left)} must be a subset of {pretty_repr(self.right)}; left had extra items {pretty_repr(self.extra)}"


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
                ...
            case first, second:
                desc = f"{first} and {second}"
            case _:  # pragma: no cover
                raise ImpossibleCaseError(case=[f"{parts=}"])
        return f"Mapping {pretty_repr(self.left)} must be a supermapping of {pretty_repr(self.right)}; {desc}"

    def _yield_parts(self) -> Iterator[str]:
        if len(self.extra) >= 1:
            yield f"right had extra keys {pretty_repr(self.extra)}"
        if len(self.errors) >= 1:
            errors = [(f"{k=}", lv, rv) for k, lv, rv in self.errors]
            yield f"differing values were {pretty_repr(errors)}"


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
        return f"Set {pretty_repr(self.left)} must be a superset of {pretty_repr(self.right)}; right had extra items {pretty_repr(self.extra)}."


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
        return f"Strings {pretty_repr(self.keys)} must not contain duplicates; got {pretty_repr(self.counts)}"


@dataclass(kw_only=True, slots=True)
class _CheckUniqueModuloCaseDuplicateLowerCaseStringsError(CheckUniqueModuloCaseError):
    values: Iterable[str]

    @override
    def __str__(self) -> str:
        return f"Strings {pretty_repr(self.values)} must not contain duplicates (modulo case); got {pretty_repr(self.counts)}"


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
        case never:
            assert_never(never)


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
        return f"Object {pretty_repr(self.obj)} must be iterable"


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


def is_iterable(obj: Any, /) -> TypeGuard[Iterable[Any]]:
    """Check if an object is iterable."""
    try:
        iter(obj)
    except TypeError:
        return False
    return True


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
            ...
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
        return f"Mapping {pretty_repr(self.mapping)} keys must not contain duplicates (modulo case); got {pretty_repr(self.counts)}"


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
        return f"Iterables {pretty_repr(include)} and {pretty_repr(exclude)} must not overlap; got {pretty_repr(overlap)}"


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
        return f"Unable to sort {pretty_repr(self.x)} and {pretty_repr(self.y)}"


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
        case never:
            assert_never(never)


##


__all__ = [
    "ApplyBijectionError",
    "CheckBijectionError",
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
    "MergeStrMappingsError",
    "ResolveIncludeAndExcludeError",
    "SortIterableError",
    "always_iterable",
    "apply_bijection",
    "apply_to_tuple",
    "apply_to_varargs",
    "check_bijection",
    "check_iterables_equal",
    "check_lengths_equal",
    "check_mappings_equal",
    "check_sets_equal",
    "check_submapping",
    "check_subset",
    "check_supermapping",
    "check_superset",
    "check_unique",
    "check_unique_modulo_case",
    "cmp_nullable",
    "ensure_iterable",
    "enumerate_with_edge",
    "filter_include_and_exclude",
    "groupby_lists",
    "is_iterable",
    "is_iterable_not_str",
    "map_mapping",
    "merge_mappings",
    "merge_sets",
    "merge_str_mappings",
    "resolve_include_and_exclude",
    "sort_iterable",
]
