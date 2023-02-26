from typing import Annotated

from pandas import Index
from pandas import Series

from utilities.beartype.numpy import DTypeB
from utilities.beartype.numpy import DTypeDns
from utilities.beartype.numpy import DTypeF
from utilities.beartype.numpy import DTypeI
from utilities.beartype.numpy import DTypeO

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
