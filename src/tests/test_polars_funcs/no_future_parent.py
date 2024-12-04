from collections.abc import Iterator
from typing import Generic, TypeVar

from polars import DataFrame

from utilities.dataclasses import Dataclass
from utilities.polars import yield_rows_as_dataclasses

_TDataclass = TypeVar("_TDataclass", bound=Dataclass)


class Parent(Generic[_TDataclass]):
    @property
    def _cls(self) -> type[_TDataclass]:
        raise NotImplementedError

    def yield_rows(self, df: DataFrame, /) -> Iterator[_TDataclass]:
        return yield_rows_as_dataclasses(df, self._cls)
