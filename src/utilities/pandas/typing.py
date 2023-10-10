from __future__ import annotations  # pragma: no cover

import datetime as dt  # pragma: no cover
from typing import TYPE_CHECKING  # pragma: no cover
from typing import Any  # pragma: no cover
from typing import TypeAlias  # pragma: no cover

from pandas import BooleanDtype  # pragma: no cover
from pandas import CategoricalDtype  # pragma: no cover
from pandas import DatetimeTZDtype  # pragma: no cover
from pandas import Index  # pragma: no cover
from pandas import Int64Dtype  # pragma: no cover
from pandas import Series  # pragma: no cover
from pandas import StringDtype  # pragma: no cover

if TYPE_CHECKING:  # pragma: no cover
    IndexA: TypeAlias = Index[Any]
    IndexB: TypeAlias = Index[bool]
    IndexBn: TypeAlias = Index[BooleanDtype]
    IndexC: TypeAlias = Index[CategoricalDtype]
    IndexD: TypeAlias = Index[dt.datetime]
    IndexDhk: TypeAlias = Index[DatetimeTZDtype]
    IndexDutc: TypeAlias = Index[DatetimeTZDtype]
    IndexF: TypeAlias = Index[float]
    IndexI: TypeAlias = Index[int]
    IndexI64: TypeAlias = Index[Int64Dtype]
    IndexS: TypeAlias = Index[StringDtype]

    SeriesA: TypeAlias = Series[Any]
    SeriesB: TypeAlias = Series[bool]
    SeriesBn: TypeAlias = Series[BooleanDtype]
    SeriesC: TypeAlias = Series[CategoricalDtype]
    SeriesD: TypeAlias = Series[dt.datetime]
    SeriesDhk: TypeAlias = Series[DatetimeTZDtype]
    SeriesDutc: TypeAlias = Series[DatetimeTZDtype]
    SeriesF: TypeAlias = Series[float]
    SeriesI: TypeAlias = Series[int]
    SeriesI64: TypeAlias = Series[Int64Dtype]
    SeriesS: TypeAlias = Series[StringDtype]
