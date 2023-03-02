from typing import Annotated

from pandas import Index, Series

from utilities.beartype.numpy import DTypeB, DTypeDns, DTypeF, DTypeI, DTypeO

# index
IndexB = Annotated[Index, DTypeB]
IndexDns = Annotated[Index, DTypeDns]
IndexF = Annotated[Index, DTypeF]
IndexI = Annotated[Index, DTypeI]
IndexO = Annotated[Index, DTypeO]

# series
SeriesB = Annotated[Series, DTypeB]
SeriesDns = Annotated[Series, DTypeDns]
SeriesF = Annotated[Series, DTypeF]
SeriesI = Annotated[Series, DTypeI]
SeriesO = Annotated[Series, DTypeO]
