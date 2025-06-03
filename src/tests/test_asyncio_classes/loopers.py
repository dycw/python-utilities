from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Literal, override

from pytest import approx

from utilities.asyncio import Looper
from utilities.contextlib import suppress_super_object_attribute_error
from utilities.datetime import MILLISECOND

if TYPE_CHECKING:
    from collections.abc import Iterator

    from utilities.types import Duration

_FREQ: Duration = 10 * MILLISECOND
_BACKOFF: Duration = 100 * MILLISECOND
_REL: float = 0.75


# assert


def assert_looper_stats(
    looper: Looper[Any],
    /,
    *,
    entries: int = 0,
    core_successes: int | tuple[Literal[">="], int] = 0,
    core_failures: int = 0,
    initialization_successes: int = 0,
    initialization_failures: int = 0,
    tear_down_successes: int = 0,
    tear_down_failures: int = 0,
    restart_successes: int = 0,
    restart_failures: int = 0,
    stops: int = 0,
    rel: float = _REL,
) -> None:
    stats = looper.stats
    assert stats.entries == entries, f"{stats=}, {entries=}"
    assert stats.core_attempts == (stats.core_successes + stats.core_failures), (
        f"{stats=}"
    )
    match core_successes:
        case int():
            assert stats.core_successes == approx(core_successes, rel=rel), (
                f"{stats=}, {core_successes=}"
            )
        case ">=", int() as min_successes:
            assert stats.core_successes >= min_successes, f"{stats=}, {min_successes=}"
    assert stats.core_failures == approx(core_failures, rel=rel), (
        f"{stats=}, {core_failures=}"
    )
    assert stats.initialization_attempts == (
        stats.initialization_successes + stats.initialization_failures
    ), f"{stats=}"
    assert stats.initialization_successes == approx(
        initialization_successes, rel=rel
    ), f"{stats=}, {initialization_successes=}"
    assert stats.initialization_failures == approx(initialization_failures, rel=rel), (
        f"{stats=}, {initialization_failures=}"
    )
    assert stats.tear_down_attempts == (
        stats.tear_down_successes + stats.tear_down_failures
    ), f"{stats=}"
    assert stats.tear_down_successes == approx(tear_down_successes, rel=rel), (
        f"{stats=}, {tear_down_successes=}"
    )
    assert stats.tear_down_failures == approx(tear_down_failures, rel=rel), (
        f"{stats=}, {initialization_failures=}"
    )
    assert stats.restart_attempts == (
        stats.restart_successes + stats.restart_failures
    ), f"{stats=}"
    assert stats.restart_successes == approx(restart_successes, rel=rel), (
        f"{stats=}, {restart_successes=}"
    )
    assert stats.restart_failures == approx(restart_failures, rel=rel), (
        f"{stats=}, {restart_failures=}"
    )
    assert stats.stops == stops, f"{stats=}, {stops=}"


def assert_looper_full(
    looper: Looper[Any], /, *, stops: int = 0, rel: float = _REL
) -> None:
    assert_looper_stats(
        looper,
        entries=1,
        core_successes=99,
        initialization_successes=1,
        stops=stops,
        rel=rel,
    )


# counting looper


@dataclass(kw_only=True)
class CountingLooper(Looper[Any]):
    freq: Duration = field(default=_FREQ, repr=False)
    backoff: Duration = field(default=_BACKOFF, repr=False)
    _debug: bool = field(default=True, repr=False)
    count: int = 0
    max_count: int = 10

    @override
    async def _initialize_core(self) -> None:
        await super()._initialize_core()
        self.count = 0

    @override
    async def core(self) -> None:
        await super().core()
        self.count += 1
        if self.count >= self.max_count:
            raise CountingLooperError


class CountingLooperError(Exception): ...


# one sub looper


@dataclass(kw_only=True)
class OuterCountingLooper(CountingLooper):
    inner: CountingLooper = field(init=False, repr=False)
    inner_auto_start: bool = False

    @override
    def __post_init__(self) -> None:
        super().__post_init__()
        self.inner = CountingLooper(
            auto_start=self.inner_auto_start,
            freq=self.freq / 2,
            backoff=self.backoff / 2,
            max_count=round(self.max_count / 2),
        )

    @override
    def _yield_sub_loopers(self) -> Iterator[Looper]:
        yield from super()._yield_sub_loopers()
        yield self.inner


# two sub loopers


@dataclass(kw_only=True)
class MultipleSubLoopers(CountingLooper):
    inner1: CountingLooper = field(init=False, repr=False)
    inner2: CountingLooper = field(init=False, repr=False)
    inner1_auto_start: bool = False
    inner2_auto_start: bool = False

    @override
    def __post_init__(self) -> None:
        super().__post_init__()
        self.inner1 = CountingLooper(
            auto_start=self.inner1_auto_start,
            freq=self.freq / 2,
            backoff=self.backoff / 2,
            max_count=round(self.max_count / 2),
        )
        self.inner2 = CountingLooper(
            auto_start=self.inner2_auto_start,
            freq=self.freq / 3,
            backoff=self.backoff / 3,
            max_count=round(self.max_count / 3),
        )

    @override
    def _yield_sub_loopers(self) -> Iterator[Looper]:
        yield from super()._yield_sub_loopers()
        yield self.inner1
        yield self.inner2


# nested sub loopers


@dataclass(kw_only=True)
class Outer2CountingLooper(CountingLooper):
    middle: OuterCountingLooper = field(init=False, repr=False)
    middle_auto_start: bool = False
    inner_auto_start: bool = False

    @override
    def __post_init__(self) -> None:
        super().__post_init__()
        self.middle = OuterCountingLooper(
            auto_start=self.middle_auto_start,
            freq=self.freq / 2,
            backoff=self.backoff / 2,
            max_count=round(self.max_count / 2),
            inner_auto_start=self.inner_auto_start,
        )

    @override
    def _yield_sub_loopers(self) -> Iterator[Looper]:
        yield from super()._yield_sub_loopers()
        yield self.middle


# one mixin


@dataclass(kw_only=True)
class CounterMixin:
    freq: Duration = field(default=_FREQ, repr=False)
    backoff: Duration = field(default=_BACKOFF, repr=False)
    _debug: bool = field(default=True, repr=False)
    count: int = 0
    max_count: int = 10
    counter_auto_start: bool = False
    _counter: CountingLooper = field(init=False, repr=False)

    def __post_init__(self) -> None:
        with suppress_super_object_attribute_error():
            super().__post_init__()  # pyright: ignore[reportAttributeAccessIssue]
        self._counter = CountingLooper(
            auto_start=self.counter_auto_start,
            freq=self.freq / 2,
            backoff=self.backoff / 2,
            max_count=round(self.max_count / 2),
        )

    def _yield_sub_loopers(self) -> Iterator[Looper[Any]]:
        with suppress_super_object_attribute_error():
            yield from super()._yield_sub_loopers()  # pyright: ignore[reportAttributeAccessIssue]
        yield self._counter


@dataclass(kw_only=True)
class LooperWithCounterMixin(CounterMixin, Looper): ...


# two mixins


@dataclass(kw_only=True)
class CounterMixin1:
    freq: Duration = field(default=_FREQ, repr=False)
    backoff: Duration = field(default=_BACKOFF, repr=False)
    _debug: bool = field(default=True, repr=False)
    count: int = 0
    max_count: int = 10
    counter1_auto_start: bool = False
    _counter1: CountingLooper = field(init=False, repr=False)

    def __post_init__(self) -> None:
        with suppress_super_object_attribute_error():
            super().__post_init__()  # pyright: ignore[reportAttributeAccessIssue]
        self._counter1 = CountingLooper(
            auto_start=self.counter1_auto_start,
            freq=self.freq / 2,
            backoff=self.backoff / 2,
            max_count=round(self.max_count / 2),
        )

    def _yield_sub_loopers(self) -> Iterator[Looper[Any]]:
        with suppress_super_object_attribute_error():
            yield from super()._yield_sub_loopers()  # pyright: ignore[reportAttributeAccessIssue]
        yield self._counter1


@dataclass(kw_only=True)
class CounterMixin2:
    freq: Duration = field(default=_FREQ, repr=False)
    backoff: Duration = field(default=_BACKOFF, repr=False)
    _debug: bool = field(default=True, repr=False)
    count: int = 0
    max_count: int = 10
    counter2_auto_start: bool = False
    _counter2: CountingLooper = field(init=False, repr=False)

    def __post_init__(self) -> None:
        with suppress_super_object_attribute_error():
            super().__post_init__()  # pyright: ignore[reportAttributeAccessIssue]
        self._counter2 = CountingLooper(
            auto_start=self.counter2_auto_start,
            freq=self.freq / 3,
            backoff=self.backoff / 3,
            max_count=round(self.max_count / 3),
        )

    def _yield_sub_loopers(self) -> Iterator[Looper[Any]]:
        with suppress_super_object_attribute_error():
            yield from super()._yield_sub_loopers()  # pyright: ignore[reportAttributeAccessIssue]
        yield self._counter2


@dataclass(kw_only=True)
class LooperWithCounterMixins(CounterMixin1, CounterMixin2, Looper): ...


# queue looper


@dataclass(kw_only=True)
class QueueLooper(Looper[int]):
    @override
    async def core(self) -> None:
        await super().core()
        if not self.empty():
            _ = self.get_left_nowait()
