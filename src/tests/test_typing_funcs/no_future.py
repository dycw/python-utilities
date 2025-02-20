from dataclasses import dataclass


@dataclass(kw_only=True)
class DataClassNestedNoFutureInnerThenOuterInner:
    int_: int


@dataclass(kw_only=True)
class DataClassNestedNoFutureInnerThenOuterOuter:
    inner: DataClassNestedNoFutureInnerThenOuterInner


@dataclass(kw_only=True)
class DataClassNestedNoFutureOuterThenInnerOuter:
    inner: "DataClassNestedNoFutureOuterThenInnerInner"


@dataclass(kw_only=True)
class DataClassNestedNoFutureOuterThenInnerInner:
    int_: int
