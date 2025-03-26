from dataclasses import dataclass


@dataclass(order=True, unsafe_hash=True, kw_only=True)
class DataClassNoFutureInt:
    int_: int


@dataclass(order=True, unsafe_hash=True, kw_only=True)
class DataClassNoFutureIntDefault:
    int_: int = 0


@dataclass(kw_only=True)
class DataClassNoFutureNestedInnerFirstInner:
    int_: int


@dataclass(kw_only=True)
class DataClassNoFutureNestedInnerFirstOuter:
    inner: DataClassNoFutureNestedInnerFirstInner


@dataclass(kw_only=True)
class DataClassNoFutureNestedOuterFirstOuter:
    inner: "DataClassNestedNoFutureOuterFirstInner"


@dataclass(kw_only=True)
class DataClassNestedNoFutureOuterFirstInner:
    int_: int
