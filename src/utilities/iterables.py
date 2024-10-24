from __future__ import annotations

import reprlib
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
from dataclasses import dataclass
from enum import Enum
from functools import partial
from itertools import accumulate, chain, groupby, islice, pairwise, product
from typing import (
    TYPE_CHECKING,
    Any,
    Generic,
    Literal,
    Never,
    Self,
    TypeGuard,
    TypeVar,
    assert_never,
    cast,
    overload,
)

from typing_extensions import override

from utilities.math import (
    _CheckIntegerEqualError,
    _CheckIntegerEqualOrApproxError,
    _CheckIntegerMaxError,
    _CheckIntegerMinError,
    check_integer,
)
from utilities.sentinel import sentinel
from utilities.text import ensure_str
from utilities.types import ensure_hashable

if TYPE_CHECKING:
    from utilities.sentinel import Sentinel

_K = TypeVar("_K")
_T = TypeVar("_T")
_U = TypeVar("_U")
_V = TypeVar("_V")
_T1 = TypeVar("_T1")
_T2 = TypeVar("_T2")
_T3 = TypeVar("_T3")
_T4 = TypeVar("_T4")
_T5 = TypeVar("_T5")
_THashable = TypeVar("_THashable", bound=Hashable)
_UHashable = TypeVar("_UHashable", bound=Hashable)
MaybeIterable = _T | Iterable[_T]
IterableHashable = tuple[_THashable, ...] | frozenset[_THashable]
MaybeIterableHashable = _THashable | IterableHashable[_THashable]


def always_iterable(obj: MaybeIterable[_T], /) -> Iterable[_T]:
    """Typed version of `always_iterable`."""
    obj = cast(Any, obj)
    if isinstance(obj, str | bytes):
        return cast(list[_T], [obj])
    try:
        return iter(cast(Iterable[_T], obj))
    except TypeError:
        return cast(list[_T], [obj])


def check_bijection(mapping: Mapping[Any, Hashable], /) -> None:
    """Check if a mapping is a bijection."""
    try:
        check_duplicates(mapping.values())
    except CheckDuplicatesError as error:
        raise CheckBijectionError(mapping=mapping, counts=error.counts) from None


@dataclass(kw_only=True, slots=True)
class CheckBijectionError(Exception, Generic[_THashable]):
    mapping: Mapping[Any, _THashable]
    counts: Mapping[_THashable, int]

    @override
    def __str__(self) -> str:
        return f"Mapping {reprlib.repr(self.mapping)} must be a bijection; got duplicates {reprlib.repr(self.counts)}"


def check_duplicates(iterable: Iterable[Hashable], /) -> None:
    """Check if an iterable contains any duplicates."""
    counts = {k: v for k, v in Counter(iterable).items() if v > 1}
    if len(counts) >= 1:
        raise CheckDuplicatesError(iterable=iterable, counts=counts)


@dataclass(kw_only=True, slots=True)
class CheckDuplicatesError(Exception, Generic[_THashable]):
    iterable: Iterable[_THashable]
    counts: Mapping[_THashable, int]

    @override
    def __str__(self) -> str:
        return f"Iterable {reprlib.repr(self.iterable)} must not contain duplicates; got {reprlib.repr(self.counts)}"


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


_CheckIterablesEqualState = Literal["left_longer", "right_longer"]


@dataclass(kw_only=True, slots=True)
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
        return f"Iterables {reprlib.repr(self.left)} and {reprlib.repr(self.right)} must be equal; {desc}"

    def _yield_parts(self) -> Iterator[str]:
        if len(self.errors) >= 1:
            errors = [(f"{i=}", lv, rv) for i, lv, rv in self.errors]
            yield f"differing items were {reprlib.repr(errors)}"
        match self.state:
            case "left_longer":
                yield "left was longer"
            case "right_longer":
                yield "right was longer"
            case None:
                pass
            case _ as never:  # pyright: ignore[reportUnnecessaryComparison]
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


@dataclass(kw_only=True, slots=True)
class CheckLengthError(Exception):
    obj: Sized


@dataclass(kw_only=True, slots=True)
class _CheckLengthEqualError(CheckLengthError):
    equal: int

    @override
    def __str__(self) -> str:
        return f"Object {reprlib.repr(self.obj)} must have length {self.equal}; got {len(self.obj)}"


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
        return f"Object {reprlib.repr(self.obj)} must have {desc}; got {len(self.obj)}"


@dataclass(kw_only=True, slots=True)
class _CheckLengthMinError(CheckLengthError):
    min_: int

    @override
    def __str__(self) -> str:
        return f"Object {reprlib.repr(self.obj)} must have minimum length {self.min_}; got {len(self.obj)}"


@dataclass(kw_only=True, slots=True)
class _CheckLengthMaxError(CheckLengthError):
    max_: int

    @override
    def __str__(self) -> str:
        return f"Object {reprlib.repr(self.obj)} must have maximum length {self.max_}; got {len(self.obj)}"


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
        return f"Sized objects {reprlib.repr(self.left)} and {reprlib.repr(self.right)} must have the same length; got {len(self.left)} and {len(self.right)}"


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
        return f"Mappings {reprlib.repr(self.left)} and {reprlib.repr(self.right)} must be equal; {desc}"

    def _yield_parts(self) -> Iterator[str]:
        if len(self.left_extra) >= 1:
            yield f"left had extra keys {reprlib.repr(self.left_extra)}"
        if len(self.right_extra) >= 1:
            yield f"right had extra keys {reprlib.repr(self.right_extra)}"
        if len(self.errors) >= 1:
            errors = [(f"{k=}", lv, rv) for k, lv, rv in self.errors]
            yield f"differing values were {reprlib.repr(errors)}"


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
        return f"Sets {reprlib.repr(self.left)} and {reprlib.repr(self.right)} must be equal; {desc}"

    def _yield_parts(self) -> Iterator[str]:
        if len(self.left_extra) >= 1:
            yield f"left had extra items {reprlib.repr(self.left_extra)}"
        if len(self.right_extra) >= 1:
            yield f"right had extra items {reprlib.repr(self.right_extra)}"


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
        return f"Mapping {reprlib.repr(self.left)} must be a submapping of {reprlib.repr(self.right)}; {desc}"

    def _yield_parts(self) -> Iterator[str]:
        if len(self.extra) >= 1:
            yield f"left had extra keys {reprlib.repr(self.extra)}"
        if len(self.errors) >= 1:
            errors = [(f"{k=}", lv, rv) for k, lv, rv in self.errors]
            yield f"differing values were {reprlib.repr(errors)}"


def check_subset(left: Iterable[Any], right: Iterable[Any], /) -> None:
    """Check that a set is a subset of another set."""
    left_as_set = set(left)
    right_as_set = set(right)
    extra = left_as_set - right_as_set
    if len(extra) >= 1:
        raise CheckSubSetError(left=left_as_set, right=right_as_set, extra=extra)


@dataclass(kw_only=True, slots=True)
class CheckSubSetError(Exception, Generic[_T]):
    left: AbstractSet[_T]
    right: AbstractSet[_T]
    extra: AbstractSet[_T]

    @override
    def __str__(self) -> str:
        return f"Set {reprlib.repr(self.left)} must be a subset of {reprlib.repr(self.right)}; left had extra items {reprlib.repr(self.extra)}"


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
        return f"Mapping {reprlib.repr(self.left)} must be a supermapping of {reprlib.repr(self.right)}; {desc}"

    def _yield_parts(self) -> Iterator[str]:
        if len(self.extra) >= 1:
            yield f"right had extra keys {reprlib.repr(self.extra)}"
        if len(self.errors) >= 1:
            errors = [(f"{k=}", lv, rv) for k, lv, rv in self.errors]
            yield f"differing values were {reprlib.repr(errors)}"


def check_superset(left: Iterable[Any], right: Iterable[Any], /) -> None:
    """Check that a set is a superset of another set."""
    left_as_set = set(left)
    right_as_set = set(right)
    extra = right_as_set - left_as_set
    if len(extra) >= 1:
        raise CheckSuperSetError(left=left_as_set, right=right_as_set, extra=extra)


@dataclass(kw_only=True, slots=True)
class CheckSuperSetError(Exception, Generic[_T]):
    left: AbstractSet[_T]
    right: AbstractSet[_T]
    extra: AbstractSet[_T]

    @override
    def __str__(self) -> str:
        return f"Set {reprlib.repr(self.left)} must be a superset of {reprlib.repr(self.right)}; right had extra items {reprlib.repr(self.extra)}."


def chunked(iterable: Iterable[_T], n: int, /) -> Iterator[Sequence[_T]]:
    """Break an iterable into lists of length n."""
    return iter(partial(take, n, iter(iterable)), [])


class Collection(frozenset[_THashable]):
    """A collection of hashable, sortable items."""

    def __new__(cls, *item_or_items: MaybeIterable[_THashable]) -> Self:
        items = list(chain(*map(always_iterable, item_or_items)))
        cls.check_items(items)
        return super().__new__(cls, items)

    def __init__(self, *item_or_items: MaybeIterable[_THashable]) -> None:
        super().__init__()
        _ = item_or_items

    @override
    def __and__(self, other: MaybeIterable[_THashable], /) -> Self:
        if isinstance(other, type(self)):
            return type(self)(super().__and__(other))
        return self.__and__(type(self)(other))

    @override
    def __or__(self, other: MaybeIterable[_THashable], /) -> Self:  # pyright: ignore[reportIncompatibleMethodOverride]
        if isinstance(other, type(self)):
            return type(self)(super().__or__(other))
        return self.__or__(type(self)(other))

    @override
    def __sub__(self, other: MaybeIterable[_THashable], /) -> Self:
        if isinstance(other, type(self)):
            return type(self)(super().__sub__(other))
        return self.__sub__(type(self)(other))

    @classmethod
    def check_items(cls, items: Iterable[_THashable], /) -> None:
        _ = items

    def filter(self, func: Callable[[_THashable], bool], /) -> Self:
        return type(self)(filter(func, self))

    def map(
        self, func: Callable[[_THashable], _UHashable], /
    ) -> Collection[_UHashable]:
        values = cast(Any, map(func, self))
        return cast(Any, type(self)(values))


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


@dataclass(kw_only=True, slots=True)
class EnsureIterableError(Exception):
    obj: Any

    @override
    def __str__(self) -> str:
        return f"Object {reprlib.repr(self.obj)} must be iterable"


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
        return f"Object {reprlib.repr(self.obj)} must be iterable, but not a string"


def expanding_window(iterable: Iterable[_T], /) -> islice[list[_T]]:
    """Yield an expanding window over an iterable."""

    def func(acc: Iterable[_T], el: _T, /) -> list[_T]:
        return list(chain(acc, [el]))

    return islice(accumulate(iterable, func=func, initial=[]), 1, None)


def hashable_to_iterable(obj: _THashable | None, /) -> tuple[_THashable, ...] | None:
    """Lift a hashable singleton to an iterable of hashables."""
    return None if obj is None else (obj,)


@overload
def filter_include_and_exclude(
    iterable: Iterable[_T],
    /,
    *,
    include: MaybeIterable[_U] | None = None,
    exclude: MaybeIterable[_U] | None = None,
    key: Callable[[_T], _U],
) -> Iterable[_T]: ...
@overload
def filter_include_and_exclude(
    iterable: Iterable[_T],
    /,
    *,
    include: MaybeIterable[_T] | None = None,
    exclude: MaybeIterable[_T] | None = None,
    key: Callable[[_T], Any] | None = None,
) -> Iterable[_T]: ...
def filter_include_and_exclude(
    iterable: Iterable[_T],
    /,
    *,
    include: MaybeIterable[_U] | None = None,
    exclude: MaybeIterable[_U] | None = None,
    key: Callable[[_T], _U] | None = None,
) -> Iterable[_T]:
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


@overload
def groupby_lists(
    iterable: Iterable[_T], /, *, key: None = None
) -> Iterator[tuple[_T, list[_T]]]: ...
@overload
def groupby_lists(
    iterable: Iterable[_T], /, *, key: Callable[[_T], _U]
) -> Iterator[tuple[_U, list[_T]]]: ...
def groupby_lists(
    iterable: Iterable[_T], /, *, key: Callable[[_T], _U] | None = None
) -> Iterator[tuple[_T, list[_T]]] | Iterator[tuple[_U, list[_T]]]:
    """Yield consecutive keys and groups (as lists)."""
    if key is None:
        for k, group in groupby(iterable):
            yield k, list(group)
    else:
        for k, group in groupby(iterable, key=key):
            yield k, list(group)


def is_iterable(obj: Any, /) -> TypeGuard[Iterable[Any]]:
    """Check if an object is iterable."""
    try:
        iter(obj)
    except TypeError:
        return False
    return True


def is_iterable_not_enum(obj: Any, /) -> TypeGuard[Iterable[Any]]:
    """Check if an object is iterable, but not an Enum."""
    return is_iterable(obj) and not (isinstance(obj, type) and issubclass(obj, Enum))


def is_iterable_not_str(obj: Any, /) -> TypeGuard[Iterable[Any]]:
    """Check if an object is iterable, but not a string."""
    return is_iterable(obj) and not isinstance(obj, str)


def one(iterable: Iterable[_T], /) -> _T:
    """Return the unique value in an iterable."""
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


@dataclass(kw_only=True, slots=True)
class OneError(Exception, Generic[_T]):
    iterable: Iterable[_T]


@dataclass(kw_only=True, slots=True)
class OneEmptyError(OneError[_T]):
    @override
    def __str__(self) -> str:
        return f"Iterable {reprlib.repr(self.iterable)} must not be empty"


@dataclass(kw_only=True, slots=True)
class OneNonUniqueError(OneError[_T]):
    first: _T
    second: _T

    @override
    def __str__(self) -> str:
        return f"Iterable {reprlib.repr(self.iterable)} must contain exactly one item; got {self.first}, {self.second} and perhaps more"


def one_str(
    iterable: Iterable[str], text: str, /, *, case_sensitive: bool = True
) -> str:
    """Find the unique string in an iterable."""
    as_list = list(iterable)
    try:
        check_duplicates(as_list)
    except CheckDuplicatesError as error:
        raise _OneStrDuplicatesError(
            iterable=iterable, text=text, counts=error.counts
        ) from None
    if case_sensitive:
        try:
            return one(t for t in as_list if t == text)
        except OneEmptyError:
            raise _OneStrCaseSensitiveEmptyError(iterable=iterable, text=text) from None
    mapping = {t: t.casefold() for t in as_list}
    try:
        check_bijection(mapping)
    except CheckBijectionError as error:
        error = cast(CheckBijectionError[str], error)
        raise _OneStrCaseInsensitiveBijectionError(
            iterable=iterable, text=text, counts=error.counts
        ) from None
    try:
        return one(k for k, v in mapping.items() if v == text.casefold())
    except OneEmptyError:
        raise _OneStrCaseInsensitiveEmptyError(iterable=iterable, text=text) from None


@dataclass(kw_only=True, slots=True)
class OneStrError(Exception):
    iterable: Iterable[str]
    text: str


@dataclass(kw_only=True, slots=True)
class _OneStrDuplicatesError(OneStrError):
    counts: Mapping[str, int]

    @override
    def __str__(self) -> str:
        return f"Iterable {reprlib.repr(self.iterable)} must not contain duplicates; got {reprlib.repr(self.counts)}"


@dataclass(kw_only=True, slots=True)
class _OneStrCaseSensitiveEmptyError(OneStrError):
    @override
    def __str__(self) -> str:
        return f"Iterable {reprlib.repr(self.iterable)} does not contain {reprlib.repr(self.text)}"


@dataclass(kw_only=True, slots=True)
class _OneStrCaseInsensitiveBijectionError(OneStrError):
    counts: Mapping[str, int]

    @override
    def __str__(self) -> str:
        return f"Iterable {reprlib.repr(self.iterable)} must not contain duplicates (case insensitive); got {reprlib.repr(self.counts)}"


@dataclass(kw_only=True, slots=True)
class _OneStrCaseInsensitiveEmptyError(OneStrError):
    @override
    def __str__(self) -> str:
        return f"Iterable {reprlib.repr(self.iterable)} does not contain {reprlib.repr(self.text)} (case insensitive)"


def pairwise_tail(iterable: Iterable[_T], /) -> Iterator[tuple[_T, _T | Sentinel]]:
    """Return pairwise elements, with the last paired with the sentinel."""
    return pairwise(chain(iterable, [sentinel]))


def product_dicts(mapping: Mapping[_K, Iterable[_V]], /) -> Iterator[Mapping[_K, _V]]:
    """Return the cartesian product of the values in a mapping, as mappings."""
    keys = list(mapping)
    for values in product(*mapping.values()):
        yield cast(Mapping[_K, _V], dict(zip(keys, values, strict=True)))


def resolve_include_and_exclude(
    *,
    include: MaybeIterable[_T] | None = None,
    exclude: MaybeIterable[_T] | None = None,
) -> tuple[set[_T] | None, set[_T] | None]:
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
class ResolveIncludeAndExcludeError(Exception, Generic[_T]):
    include: Iterable[_T]
    exclude: Iterable[_T]

    @override
    def __str__(self) -> str:
        include = list(self.include)
        exclude = list(self.exclude)
        overlap = set(include) & set(exclude)
        return f"Iterables {reprlib.repr(include)} and {reprlib.repr(exclude)} must not overlap; got {reprlib.repr(overlap)}"


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
    "EnsureIterableError",
    "EnsureIterableNotStrError",
    "IterableHashable",
    "MaybeIterable",
    "MaybeIterableHashable",
    "OneEmptyError",
    "OneError",
    "OneNonUniqueError",
    "ResolveIncludeAndExcludeError",
    "always_iterable",
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
    "chunked",
    "ensure_hashables",
    "ensure_iterable",
    "ensure_iterable_not_str",
    "expanding_window",
    "filter_include_and_exclude",
    "groupby_lists",
    "hashable_to_iterable",
    "is_iterable",
    "is_iterable_not_enum",
    "is_iterable_not_str",
    "one",
    "pairwise_tail",
    "product_dicts",
    "resolve_include_and_exclude",
    "take",
    "transpose",
]
