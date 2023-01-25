from typing import TypeVar, Union

from hypothesis.strategies import SearchStrategy

_T = TypeVar("_T")
MaybeSearchStrategy = Union[_T, SearchStrategy[_T]]


Shape = Union[int, tuple[int, ...]]
