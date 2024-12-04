from collections.abc import Iterator
from dataclasses import dataclass
from typing import Literal

from polars.dataframe import DataFrame
from typing_extensions import override

from tests.test_polars_funcs.no_future_parent import Parent
from utilities.types import StrMapping

TruthLit = Literal["true", "false"]  # in 3.12, use type TruthLit = ...


@dataclass(kw_only=True, slots=True)
class ChildData:
    truth: TruthLit


class Child(Parent[ChildData]):
    @property
    @override
    def _cls(self) -> type[ChildData]:
        return ChildData

    @override
    def yield_rows_as_dataclasses(
        self,
        df: DataFrame,
        /,
        *,
        globalns: StrMapping | None = None,
        localns: StrMapping | None = None,
    ) -> Iterator[ChildData]:
        return super().yield_rows_as_dataclasses(
            df,
            globalns=globals() if globalns is None else globalns,
            localns=locals() if localns is None else localns,
        )
