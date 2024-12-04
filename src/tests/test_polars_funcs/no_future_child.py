from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal

from typing_extensions import override

from tests.test_polars_funcs.no_future_parent import Parent

if TYPE_CHECKING:
    from collections.abc import Iterator

    from polars.dataframe import DataFrame

    from utilities.types import StrMapping

TruthLit = Literal["true", "false"]  # in 3.12, use type TruthLit = ...


@dataclass(kw_only=True, slots=True)
class Data:
    truth: TruthLit


class Child(Parent[Data]):
    @property
    @override
    def _cls(self) -> type[Data]:
        return Data

    @override
    def yield_rows(
        self,
        df: DataFrame,
        /,
        *,
        globalns: StrMapping | None = None,
        localns: StrMapping | None = None,
    ) -> Iterator[Data]:
        return super().yield_rows(
            df,
            globalns=globals() if globalns is None else globalns,
            localns=locals() if localns is None else localns,
        )
