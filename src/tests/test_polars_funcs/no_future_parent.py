from abc import ABC, abstractmethod
from collections.abc import Iterator
from typing import Generic, TypeVar

from polars import DataFrame

from utilities.dataclasses import Dataclass
from utilities.polars import yield_rows_as_dataclasses
from utilities.types import StrMapping

_TDataclass = TypeVar("_TDataclass", bound=Dataclass)


class Parent(ABC, Generic[_TDataclass]):
    @property
    @abstractmethod
    def _cls(self) -> type[_TDataclass]:
        raise NotImplementedError

    def yield_rows(
        self,
        df: DataFrame,
        /,
        *,
        globalns: StrMapping | None = None,
        localns: StrMapping | None = None,
    ) -> Iterator[_TDataclass]:
        return yield_rows_as_dataclasses(
            df, self._cls, globalns=globalns, localns=localns
        )
