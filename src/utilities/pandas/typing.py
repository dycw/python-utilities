from __future__ import annotations

import datetime as dt
from typing import Annotated, Any

from beartype.vale import IsAttr, IsEqual
from pandas import (
    BooleanDtype,
    CategoricalDtype,
    DatetimeTZDtype,
    Index,
    Int64Dtype,
    Series,
    StringDtype,
)
from typing_extensions import TypeAlias

from utilities.datetime import UTC
from utilities.numpy.typing import DTypeB, DTypeDns, DTypeF, DTypeI, DTypeO
from utilities.zoneinfo import HONG_KONG

# dtypes
Int64 = "Int64"
boolean = "boolean"
category = "category"
string = "string"
datetime64nsutc = DatetimeTZDtype(tz=UTC)
datetime64nshk = DatetimeTZDtype(tz=HONG_KONG)

# dtype checkers
DTypeBn = IsAttr["dtype", IsEqual[boolean]]
DTypeC = IsAttr["dtype", IsEqual[category]]
DTypeI64 = IsAttr["dtype", IsEqual[Int64]]
DTypeS = IsAttr["dtype", IsEqual[string]]
DTypeDutc = IsAttr["dtype", IsEqual[datetime64nsutc]]
DTypeDhk = IsAttr["dtype", IsEqual[datetime64nshk]]

# annotated; index
IndexB: TypeAlias = Annotated[Index[bool], DTypeB]
IndexBn: TypeAlias = Annotated[Index[BooleanDtype], DTypeBn]
IndexC: TypeAlias = Annotated[Index[CategoricalDtype], DTypeC]
IndexD: TypeAlias = Annotated[Index[dt.datetime], DTypeDns]
IndexDhk: TypeAlias = Annotated[Index[DatetimeTZDtype], DTypeDhk]
IndexDutc: TypeAlias = Annotated[Index[DatetimeTZDtype], DTypeDutc]
IndexF: TypeAlias = Annotated[Index[float], DTypeF]
IndexI: TypeAlias = Annotated[Index[int], DTypeI]
IndexI64: TypeAlias = Annotated[Index[Int64Dtype], DTypeI64]
IndexO: TypeAlias = Annotated[Index[Any], DTypeO]
IndexS: TypeAlias = Annotated[Index[StringDtype], DTypeS]

# series annotated;
SeriesB: TypeAlias = Annotated[Series[bool], DTypeB]
SeriesBn: TypeAlias = Annotated[Series[BooleanDtype], DTypeBn]
SeriesC: TypeAlias = Annotated[Series[CategoricalDtype], DTypeC]
SeriesD: TypeAlias = Annotated[Series[dt.datetime], DTypeDns]
SeriesDhk: TypeAlias = Annotated[Series[DatetimeTZDtype], DTypeDhk]
SeriesDutc: TypeAlias = Annotated[Series[DatetimeTZDtype], DTypeDutc]
SeriesF: TypeAlias = Annotated[Series[float], DTypeF]
SeriesI: TypeAlias = Annotated[Series[int], DTypeI]
SeriesI64: TypeAlias = Annotated[Series[Int64Dtype], DTypeI64]
SeriesO: TypeAlias = Annotated[Series[Any], DTypeO]
SeriesS: TypeAlias = Annotated[Series[StringDtype], DTypeS]
