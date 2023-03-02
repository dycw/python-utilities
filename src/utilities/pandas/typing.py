from typing import Annotated

from pandas import Index, Series
from typing_extensions import TypeAlias

from utilities.beartype.numpy import DTypeB, DTypeDns, DTypeF, DTypeI, DTypeO
from utilities.beartype.pandas import DTypeBoolean, DTypeInt64, DTypeString

# index
IndexB: TypeAlias = Annotated[Index, DTypeB]
IndexBn: TypeAlias = Annotated[Index, DTypeBoolean]
IndexDns: TypeAlias = Annotated[Index, DTypeDns]
IndexF: TypeAlias = Annotated[Index, DTypeF]
IndexI64: TypeAlias = Annotated[Index, DTypeInt64]
IndexI: TypeAlias = Annotated[Index, DTypeI]
IndexO: TypeAlias = Annotated[Index, DTypeO]
IndexS: TypeAlias = Annotated[Index, DTypeString]

# series
SeriesB: TypeAlias = Annotated[Series, DTypeB]
SeriesBn: TypeAlias = Annotated[Series, DTypeBoolean]
SeriesDns: TypeAlias = Annotated[Series, DTypeDns]
SeriesF: TypeAlias = Annotated[Series, DTypeF]
SeriesI: TypeAlias = Annotated[Series, DTypeI]
SeriesI64: TypeAlias = Annotated[Series, DTypeInt64]
SeriesO: TypeAlias = Annotated[Series, DTypeO]
SeriesS: TypeAlias = Annotated[Series, DTypeString]
