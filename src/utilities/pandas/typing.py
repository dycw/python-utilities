from typing import Annotated

from beartype.vale import IsAttr, IsEqual
from pandas import DatetimeTZDtype, Index, Series
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
IndexB: TypeAlias = Annotated[Index, DTypeB]
IndexBn: TypeAlias = Annotated[Index, DTypeBn]
IndexC: TypeAlias = Annotated[Index, DTypeC]
IndexD: TypeAlias = Annotated[Index, DTypeDns]
IndexDhk: TypeAlias = Annotated[Index, DTypeDhk]
IndexDutc: TypeAlias = Annotated[Index, DTypeDutc]
IndexF: TypeAlias = Annotated[Index, DTypeF]
IndexI64: TypeAlias = Annotated[Index, DTypeI64]
IndexI: TypeAlias = Annotated[Index, DTypeI]
IndexO: TypeAlias = Annotated[Index, DTypeO]
IndexS: TypeAlias = Annotated[Index, DTypeS]

# series annotated;
SeriesB: TypeAlias = Annotated[Series, DTypeB]
SeriesBn: TypeAlias = Annotated[Series, DTypeBn]
SeriesC: TypeAlias = Annotated[Series, DTypeC]
SeriesD: TypeAlias = Annotated[Series, DTypeDns]
SeriesDhk: TypeAlias = Annotated[Series, DTypeDhk]
SeriesDutc: TypeAlias = Annotated[Series, DTypeDutc]
SeriesF: TypeAlias = Annotated[Series, DTypeF]
SeriesI: TypeAlias = Annotated[Series, DTypeI]
SeriesI64: TypeAlias = Annotated[Series, DTypeI64]
SeriesO: TypeAlias = Annotated[Series, DTypeO]
SeriesS: TypeAlias = Annotated[Series, DTypeS]
