from __future__ import annotations

from collections.abc import Callable
from typing import Annotated, Any, Union, cast

from beartype.vale import Is, IsAttr, IsEqual
from numpy import bool_, float64, int64, isfinite, log, object_, unravel_index
from numpy.random import default_rng
from numpy.typing import NDArray

from utilities.beartype import NDim0, NDim1, NDim2, NDim3
from utilities.numpy.checks import (
    is_finite_and_integral,
    is_finite_and_integral_or_nan,
    is_finite_and_negative,
    is_finite_and_negative_or_nan,
    is_finite_and_non_negative,
    is_finite_and_non_negative_or_nan,
    is_finite_and_non_positive,
    is_finite_and_non_positive_or_nan,
    is_finite_and_non_zero,
    is_finite_and_non_zero_or_nan,
    is_finite_and_positive,
    is_finite_and_positive_or_nan,
    is_finite_or_nan,
    is_integral,
    is_integral_or_nan,
    is_negative,
    is_negative_or_nan,
    is_non_negative,
    is_non_negative_or_nan,
    is_non_positive,
    is_non_positive_or_nan,
    is_non_zero,
    is_non_zero_or_nan,
    is_positive,
    is_positive_or_nan,
    is_zero,
    is_zero_or_finite_and_non_micro,
    is_zero_or_finite_and_non_micro_or_nan,
    is_zero_or_nan,
    is_zero_or_non_micro,
    is_zero_or_non_micro_or_nan,
)
from utilities.numpy.dtypes import (
    datetime64as,
    datetime64D,
    datetime64fs,
    datetime64h,
    datetime64M,
    datetime64m,
    datetime64ms,
    datetime64ns,
    datetime64ps,
    datetime64s,
    datetime64us,
    datetime64W,
    datetime64Y,
)

# dtype checkers
DTypeB = IsAttr["dtype", IsEqual[bool]]
DTypeDY = IsAttr["dtype", IsEqual[datetime64Y]]
DTypeDM = IsAttr["dtype", IsEqual[datetime64M]]
DTypeDW = IsAttr["dtype", IsEqual[datetime64W]]
DTypeDD = IsAttr["dtype", IsEqual[datetime64D]]
DTypeDh = IsAttr["dtype", IsEqual[datetime64h]]
DTypeDm = IsAttr["dtype", IsEqual[datetime64m]]
DTypeDs = IsAttr["dtype", IsEqual[datetime64s]]
DTypeDms = IsAttr["dtype", IsEqual[datetime64ms]]
DTypeDus = IsAttr["dtype", IsEqual[datetime64us]]
DTypeDns = IsAttr["dtype", IsEqual[datetime64ns]]
DTypeDps = IsAttr["dtype", IsEqual[datetime64ps]]
DTypeDfs = IsAttr["dtype", IsEqual[datetime64fs]]
DTypeDas = IsAttr["dtype", IsEqual[datetime64as]]
DTypeD = (
    DTypeDY
    | DTypeDM
    | DTypeDW
    | DTypeDD
    | DTypeDh
    | DTypeDm
    | DTypeDs
    | DTypeDms
    | DTypeDus
    | DTypeDns
    | DTypeDps
    | DTypeDfs
    | DTypeDas
)
DTypeF = IsAttr["dtype", IsEqual[float]]
DTypeI = IsAttr["dtype", IsEqual[int]]
DTypeO = IsAttr["dtype", IsEqual[object]]

# annotated; dtype
NDArrayA = NDArray[Any]
NDArrayB = NDArray[bool_]
NDArrayDY = NDArray[cast(Any, datetime64Y)]
NDArrayDM = NDArray[cast(Any, datetime64M)]
NDArrayDW = NDArray[cast(Any, datetime64W)]
NDArrayDD = NDArray[cast(Any, datetime64D)]
NDArrayDh = NDArray[cast(Any, datetime64h)]
NDArrayDm = NDArray[cast(Any, datetime64m)]
NDArrayDs = NDArray[cast(Any, datetime64s)]
NDArrayDms = NDArray[cast(Any, datetime64ms)]
NDArrayDus = NDArray[cast(Any, datetime64us)]
NDArrayDns = NDArray[cast(Any, datetime64ns)]
NDArrayDps = NDArray[cast(Any, datetime64ps)]
NDArrayDfs = NDArray[cast(Any, datetime64fs)]
NDArrayDas = NDArray[cast(Any, datetime64as)]
NDArrayD = Union[
    NDArrayDY,
    NDArrayDM,
    NDArrayDW,
    NDArrayDD,
    NDArrayDh,
    NDArrayDm,
    NDArrayDs,
    NDArrayDms,
    NDArrayDus,
    NDArrayDns,
    NDArrayDps,
    NDArrayDfs,
    NDArrayDas,
]
NDArrayF = NDArray[float64]
NDArrayI = NDArray[int64]
NDArrayO = NDArray[object_]

# annotated; ndim
NDArray0 = Annotated[NDArrayA, NDim0]
NDArray1 = Annotated[NDArrayA, NDim1]
NDArray2 = Annotated[NDArrayA, NDim2]
NDArray3 = Annotated[NDArrayA, NDim3]

# annotated; dtype & ndim
NDArrayB0 = Annotated[NDArrayB, NDim0]
NDArrayD0 = Annotated[NDArrayD, NDim0]
NDArrayDY0 = Annotated[NDArrayDY, NDim0]
NDArrayDM0 = Annotated[NDArrayDM, NDim0]
NDArrayDW0 = Annotated[NDArrayDW, NDim0]
NDArrayDD0 = Annotated[NDArrayDD, NDim0]
NDArrayDh0 = Annotated[NDArrayDh, NDim0]
NDArrayDm0 = Annotated[NDArrayDm, NDim0]
NDArrayDs0 = Annotated[NDArrayDs, NDim0]
NDArrayDms0 = Annotated[NDArrayDms, NDim0]
NDArrayDus0 = Annotated[NDArrayDus, NDim0]
NDArrayDns0 = Annotated[NDArrayDns, NDim0]
NDArrayDps0 = Annotated[NDArrayDps, NDim0]
NDArrayDfs0 = Annotated[NDArrayDfs, NDim0]
NDArrayDas0 = Annotated[NDArrayDas, NDim0]
NDArrayF0 = Annotated[NDArrayF, NDim0]
NDArrayI0 = Annotated[NDArrayI, NDim0]
NDArrayO0 = Annotated[NDArrayO, NDim0]

NDArrayB1 = Annotated[NDArrayB, NDim1]
NDArrayD1 = Annotated[NDArrayD, NDim1]
NDArrayDY1 = Annotated[NDArrayDY, NDim1]
NDArrayDM1 = Annotated[NDArrayDM, NDim1]
NDArrayDW1 = Annotated[NDArrayDW, NDim1]
NDArrayDD1 = Annotated[NDArrayDD, NDim1]
NDArrayDh1 = Annotated[NDArrayDh, NDim1]
NDArrayDm1 = Annotated[NDArrayDm, NDim1]
NDArrayDs1 = Annotated[NDArrayDs, NDim1]
NDArrayDms1 = Annotated[NDArrayDms, NDim1]
NDArrayDus1 = Annotated[NDArrayDus, NDim1]
NDArrayDns1 = Annotated[NDArrayDns, NDim1]
NDArrayDps1 = Annotated[NDArrayDps, NDim1]
NDArrayDfs1 = Annotated[NDArrayDfs, NDim1]
NDArrayDas1 = Annotated[NDArrayDas, NDim1]
NDArrayF1 = Annotated[NDArrayF, NDim1]
NDArrayI1 = Annotated[NDArrayI, NDim1]
NDArrayO1 = Annotated[NDArrayO, NDim1]

NDArrayB2 = Annotated[NDArrayB, NDim2]
NDArrayD2 = Annotated[NDArrayD, NDim2]
NDArrayDY2 = Annotated[NDArrayDY, NDim2]
NDArrayDM2 = Annotated[NDArrayDM, NDim2]
NDArrayDW2 = Annotated[NDArrayDW, NDim2]
NDArrayDD2 = Annotated[NDArrayDD, NDim2]
NDArrayDh2 = Annotated[NDArrayDh, NDim2]
NDArrayDm2 = Annotated[NDArrayDm, NDim2]
NDArrayDs2 = Annotated[NDArrayDs, NDim2]
NDArrayDms2 = Annotated[NDArrayDms, NDim2]
NDArrayDus2 = Annotated[NDArrayDus, NDim2]
NDArrayDns2 = Annotated[NDArrayDns, NDim2]
NDArrayDps2 = Annotated[NDArrayDps, NDim2]
NDArrayDfs2 = Annotated[NDArrayDfs, NDim2]
NDArrayDas2 = Annotated[NDArrayDas, NDim2]
NDArrayF2 = Annotated[NDArrayF, NDim2]
NDArrayI2 = Annotated[NDArrayI, NDim2]
NDArrayO2 = Annotated[NDArrayO, NDim2]

NDArrayB3 = Annotated[NDArrayB, NDim3]
NDArrayD3 = Annotated[NDArrayD, NDim3]
NDArrayDY3 = Annotated[NDArrayDY, NDim3]
NDArrayDM3 = Annotated[NDArrayDM, NDim3]
NDArrayDW3 = Annotated[NDArrayDW, NDim3]
NDArrayDD3 = Annotated[NDArrayDD, NDim3]
NDArrayDh3 = Annotated[NDArrayDh, NDim3]
NDArrayDm3 = Annotated[NDArrayDm, NDim3]
NDArrayDs3 = Annotated[NDArrayDs, NDim3]
NDArrayDms3 = Annotated[NDArrayDms, NDim3]
NDArrayDus3 = Annotated[NDArrayDus, NDim3]
NDArrayDns3 = Annotated[NDArrayDns, NDim3]
NDArrayDps3 = Annotated[NDArrayDps, NDim3]
NDArrayDfs3 = Annotated[NDArrayDfs, NDim3]
NDArrayDas3 = Annotated[NDArrayDas, NDim3]
NDArrayF3 = Annotated[NDArrayF, NDim3]
NDArrayI3 = Annotated[NDArrayI, NDim3]
NDArrayO3 = Annotated[NDArrayO, NDim3]


def _lift(check: Callable[..., Any], /) -> Any:
    """Lift a check to work on a subset of a float array."""
    rng = default_rng()

    def predicate(array: NDArrayI | NDArrayF, /) -> bool:
        if (size := array.size) == 0:
            return True
        if size == 1:
            return check(array).item()
        num_samples = round(log(size))
        indices = rng.integers(0, size, size=num_samples)
        sample = array[unravel_index(indices, array.shape)]
        return check(sample).all().item()

    return Is[cast(Any, predicate)]


_is_finite = _lift(isfinite)
_is_finite_and_integral = _lift(is_finite_and_integral)
_is_finite_and_integral_or_nan = _lift(is_finite_and_integral_or_nan)
_is_finite_and_negative = _lift(is_finite_and_negative)
_is_finite_and_negative_or_nan = _lift(is_finite_and_negative_or_nan)
_is_finite_and_non_negative = _lift(is_finite_and_non_negative)
_is_finite_and_non_negative_or_nan = _lift(is_finite_and_non_negative_or_nan)
_is_finite_and_non_positive = _lift(is_finite_and_non_positive)
_is_finite_and_non_positive_or_nan = _lift(is_finite_and_non_positive_or_nan)
_is_finite_and_non_zero = _lift(is_finite_and_non_zero)
_is_finite_and_non_zero_or_nan = _lift(is_finite_and_non_zero_or_nan)
_is_finite_and_positive = _lift(is_finite_and_positive)
_is_finite_and_positive_or_nan = _lift(is_finite_and_positive_or_nan)
_is_finite_or_nan = _lift(is_finite_or_nan)
_is_integral = _lift(is_integral)
_is_integral_or_nan = _lift(is_integral_or_nan)
_is_negative = _lift(is_negative)
_is_negative_or_nan = _lift(is_negative_or_nan)
_is_non_negative = _lift(is_non_negative)
_is_non_negative_or_nan = _lift(is_non_negative_or_nan)
_is_non_positive = _lift(is_non_positive)
_is_non_positive_or_nan = _lift(is_non_positive_or_nan)
_is_non_zero = _lift(is_non_zero)
_is_non_zero_or_nan = _lift(is_non_zero_or_nan)
_is_positive = _lift(is_positive)
_is_positive_or_nan = _lift(is_positive_or_nan)
_is_zero = _lift(is_zero)
_is_zero_or_finite_and_non_micro = _lift(is_zero_or_finite_and_non_micro)
_is_zero_or_finite_and_non_micro_or_nan = _lift(
    is_zero_or_finite_and_non_micro_or_nan
)
_is_zero_or_nan = _lift(is_zero_or_nan)
_is_zero_or_non_micro = _lift(is_zero_or_non_micro)
_is_zero_or_non_micro_or_nan = _lift(is_zero_or_non_micro_or_nan)


# annotated; int & checks
NDArrayINeg = Annotated[NDArrayI, _is_negative]
NDArrayINonNeg = Annotated[NDArrayI, _is_non_negative]
NDArrayINonPos = Annotated[NDArrayI, _is_non_positive]
NDArrayINonZr = Annotated[NDArrayI, _is_non_zero]
NDArrayIPos = Annotated[NDArrayI, _is_positive]
NDArrayIZr = Annotated[NDArrayI, _is_zero]


# annotated; float & checks
NDArrayFFin = Annotated[NDArrayF, _is_finite]
NDArrayFFinInt = Annotated[NDArrayF, _is_finite_and_integral]
NDArrayFFinIntNan = Annotated[NDArrayF, _is_finite_and_integral_or_nan]
NDArrayFFinNeg = Annotated[NDArrayF, _is_finite_and_negative]
NDArrayFFinNegNan = Annotated[NDArrayF, _is_finite_and_negative_or_nan]
NDArrayFFinNonNeg = Annotated[NDArrayF, _is_finite_and_non_negative]
NDArrayFFinNonNegNan = Annotated[NDArrayF, _is_finite_and_non_negative_or_nan]
NDArrayFFinNonPos = Annotated[NDArrayF, _is_finite_and_non_positive]
NDArrayFFinNonPosNan = Annotated[NDArrayF, _is_finite_and_non_positive_or_nan]
NDArrayFFinNonZr = Annotated[NDArrayF, _is_finite_and_non_zero]
NDArrayFFinNonZrNan = Annotated[NDArrayF, _is_finite_and_non_zero_or_nan]
NDArrayFFinPos = Annotated[NDArrayF, _is_finite_and_positive]
NDArrayFFinPosNan = Annotated[NDArrayF, _is_finite_and_positive_or_nan]
NDArrayFFinNan = Annotated[NDArrayF, _is_finite_or_nan]
NDArrayFInt = Annotated[NDArrayF, _is_integral]
NDArrayFIntNan = Annotated[NDArrayF, _is_integral_or_nan]
NDArrayFNeg = Annotated[NDArrayF, _is_negative]
NDArrayFNegNan = Annotated[NDArrayF, _is_negative_or_nan]
NDArrayFNonNeg = Annotated[NDArrayF, _is_non_negative]
NDArrayFNonNegNan = Annotated[NDArrayF, _is_non_negative_or_nan]
NDArrayFNonPos = Annotated[NDArrayF, _is_non_positive]
NDArrayFNonPosNan = Annotated[NDArrayF, _is_non_positive_or_nan]
NDArrayFNonZr = Annotated[NDArrayF, _is_non_zero]
NDArrayFNonZrNan = Annotated[NDArrayF, _is_non_zero_or_nan]
NDArrayFPos = Annotated[NDArrayF, _is_positive]
NDArrayFPosNan = Annotated[NDArrayF, _is_positive_or_nan]
NDArrayFZr = Annotated[NDArrayF, _is_zero]
NDArrayFZrFinNonMic = Annotated[NDArrayF, _is_zero_or_finite_and_non_micro]
NDArrayFZrFinNonMicNan = Annotated[
    NDArrayF, _is_zero_or_finite_and_non_micro_or_nan
]
NDArrayFZrNan = Annotated[NDArrayF, _is_zero_or_nan]
NDArrayFZrNonMic = Annotated[NDArrayF, _is_zero_or_non_micro]
NDArrayFZrNonMicNan = Annotated[NDArrayF, _is_zero_or_non_micro_or_nan]

# annotated; int, ndim & checks
NDArrayI0Neg = Annotated[NDArrayI, NDim0 & _is_negative]
NDArrayI0NonNeg = Annotated[NDArrayI, NDim0 & _is_non_negative]
NDArrayI0NonPos = Annotated[NDArrayI, NDim0 & _is_non_positive]
NDArrayI0NonZr = Annotated[NDArrayI, NDim0 & _is_non_zero]
NDArrayI0Pos = Annotated[NDArrayI, NDim0 & _is_positive]
NDArrayI0Zr = Annotated[NDArrayI, NDim0 & _is_zero]

NDArrayI1Neg = Annotated[NDArrayI, NDim1 & _is_negative]
NDArrayI1NonNeg = Annotated[NDArrayI, NDim1 & _is_non_negative]
NDArrayI1NonPos = Annotated[NDArrayI, NDim1 & _is_non_positive]
NDArrayI1NonZr = Annotated[NDArrayI, NDim1 & _is_non_zero]
NDArrayI1Pos = Annotated[NDArrayI, NDim1 & _is_positive]
NDArrayI1Zr = Annotated[NDArrayI, NDim1 & _is_zero]

NDArrayI2Neg = Annotated[NDArrayI, NDim2 & _is_negative]
NDArrayI2NonNeg = Annotated[NDArrayI, NDim2 & _is_non_negative]
NDArrayI2NonPos = Annotated[NDArrayI, NDim2 & _is_non_positive]
NDArrayI2NonZr = Annotated[NDArrayI, NDim2 & _is_non_zero]
NDArrayI2Pos = Annotated[NDArrayI, NDim2 & _is_positive]
NDArrayI2Zr = Annotated[NDArrayI, NDim2 & _is_zero]

NDArrayI3Neg = Annotated[NDArrayI, NDim1 & _is_negative]
NDArrayI3NonNeg = Annotated[NDArrayI, NDim3 & _is_non_negative]
NDArrayI3NonPos = Annotated[NDArrayI, NDim3 & _is_non_positive]
NDArrayI3NonZr = Annotated[NDArrayI, NDim3 & _is_non_zero]
NDArrayI3Pos = Annotated[NDArrayI, NDim3 & _is_positive]
NDArrayI3Zr = Annotated[NDArrayI, NDim3 & _is_zero]

# annotated; float, ndim & checks
NDArrayF0Fin = Annotated[NDArrayF, NDim0 & _is_finite]
NDArrayF0FinInt = Annotated[NDArrayF, NDim0 & _is_finite_and_integral]
NDArrayF0FinIntNan = Annotated[NDArrayF, NDim0 & _is_finite_and_integral_or_nan]
NDArrayF0FinNeg = Annotated[NDArrayF, NDim0 & _is_finite_and_negative]
NDArrayF0FinNegNan = Annotated[NDArrayF, NDim0 & _is_finite_and_negative_or_nan]
NDArrayF0FinNonNeg = Annotated[NDArrayF, NDim0 & _is_finite_and_non_negative]
NDArrayF0FinNonNegNan = Annotated[
    NDArrayF, NDim0 & _is_finite_and_non_negative_or_nan
]
NDArrayF0FinNonPos = Annotated[NDArrayF, NDim0 & _is_finite_and_non_positive]
NDArrayF0FinNonPosNan = Annotated[
    NDArrayF, NDim0 & _is_finite_and_non_positive_or_nan
]
NDArrayF0FinNonZr = Annotated[NDArrayF, NDim0 & _is_finite_and_non_zero]
NDArrayF0FinNonZrNan = Annotated[
    NDArrayF, NDim0 & _is_finite_and_non_zero_or_nan
]
NDArrayF0FinPos = Annotated[NDArrayF, NDim0 & _is_finite_and_positive]
NDArrayF0FinPosNan = Annotated[NDArrayF, NDim0 & _is_finite_and_positive_or_nan]
NDArrayF0FinNan = Annotated[NDArrayF, NDim0 & _is_finite_or_nan]
NDArrayF0Int = Annotated[NDArrayF, NDim0 & _is_integral]
NDArrayF0IntNan = Annotated[NDArrayF, NDim0 & _is_integral_or_nan]
NDArrayF0Neg = Annotated[NDArrayF, NDim0 & _is_negative]
NDArrayF0NegNan = Annotated[NDArrayF, NDim0 & _is_negative_or_nan]
NDArrayF0NonNeg = Annotated[NDArrayF, NDim0 & _is_non_negative]
NDArrayF0NonNegNan = Annotated[NDArrayF, NDim0 & _is_non_negative_or_nan]
NDArrayF0NonPos = Annotated[NDArrayF, NDim0 & _is_non_positive]
NDArrayF0NonPosNan = Annotated[NDArrayF, NDim0 & _is_non_positive_or_nan]
NDArrayF0NonZr = Annotated[NDArrayF, NDim0 & _is_non_zero]
NDArrayF0NonZrNan = Annotated[NDArrayF, NDim0 & _is_non_zero_or_nan]
NDArrayF0Pos = Annotated[NDArrayF, NDim0 & _is_positive]
NDArrayF0PosNan = Annotated[NDArrayF, NDim0 & _is_positive_or_nan]
NDArrayF0Zr = Annotated[NDArrayF, NDim0 & _is_zero]
NDArrayF0ZrFinNonMic = Annotated[
    NDArrayF, NDim0 & _is_zero_or_finite_and_non_micro
]
NDArrayF0ZrFinNonMicNan = Annotated[
    NDArrayF, NDim0 & _is_zero_or_finite_and_non_micro_or_nan
]
NDArrayF0ZrNan = Annotated[NDArrayF, NDim0 & _is_zero_or_nan]
NDArrayF0ZrNonMic = Annotated[NDArrayF, NDim0 & _is_zero_or_non_micro]
NDArrayF0ZrNonMicNan = Annotated[NDArrayF, NDim0 & _is_zero_or_non_micro_or_nan]

NDArrayF1Fin = Annotated[NDArrayF, NDim1 & _is_finite]
NDArrayF1FinInt = Annotated[NDArrayF, NDim1 & _is_finite_and_integral]
NDArrayF1FinIntNan = Annotated[NDArrayF, NDim1 & _is_finite_and_integral_or_nan]
NDArrayF1FinNeg = Annotated[NDArrayF, NDim1 & _is_finite_and_negative]
NDArrayF1FinNegNan = Annotated[NDArrayF, NDim1 & _is_finite_and_negative_or_nan]
NDArrayF1FinNonNeg = Annotated[NDArrayF, NDim1 & _is_finite_and_non_negative]
NDArrayF1FinNonNegNan = Annotated[
    NDArrayF, NDim1 & _is_finite_and_non_negative_or_nan
]
NDArrayF1FinNonPos = Annotated[NDArrayF, NDim1 & _is_finite_and_non_positive]
NDArrayF1FinNonPosNan = Annotated[
    NDArrayF, NDim1 & _is_finite_and_non_positive_or_nan
]
NDArrayF1FinNonZr = Annotated[NDArrayF, NDim1 & _is_finite_and_non_zero]
NDArrayF1FinNonZrNan = Annotated[
    NDArrayF, NDim1 & _is_finite_and_non_zero_or_nan
]
NDArrayF1FinPos = Annotated[NDArrayF, NDim1 & _is_finite_and_positive]
NDArrayF1FinPosNan = Annotated[NDArrayF, NDim1 & _is_finite_and_positive_or_nan]
NDArrayF1FinNan = Annotated[NDArrayF, NDim1 & _is_finite_or_nan]
NDArrayF1Int = Annotated[NDArrayF, NDim1 & _is_integral]
NDArrayF1IntNan = Annotated[NDArrayF, NDim1 & _is_integral_or_nan]
NDArrayF1Neg = Annotated[NDArrayF, NDim1 & _is_negative]
NDArrayF1NegNan = Annotated[NDArrayF, NDim1 & _is_negative_or_nan]
NDArrayF1NonNeg = Annotated[NDArrayF, NDim1 & _is_non_negative]
NDArrayF1NonNegNan = Annotated[NDArrayF, NDim1 & _is_non_negative_or_nan]
NDArrayF1NonPos = Annotated[NDArrayF, NDim1 & _is_non_positive]
NDArrayF1NonPosNan = Annotated[NDArrayF, NDim1 & _is_non_positive_or_nan]
NDArrayF1NonZr = Annotated[NDArrayF, NDim1 & _is_non_zero]
NDArrayF1NonZrNan = Annotated[NDArrayF, NDim1 & _is_non_zero_or_nan]
NDArrayF1Pos = Annotated[NDArrayF, NDim1 & _is_positive]
NDArrayF1PosNan = Annotated[NDArrayF, NDim1 & _is_positive_or_nan]
NDArrayF1Zr = Annotated[NDArrayF, NDim1 & _is_zero]
NDArrayF1ZrFinNonMic = Annotated[
    NDArrayF, NDim1 & _is_zero_or_finite_and_non_micro
]
NDArrayF1ZrFinNonMicNan = Annotated[
    NDArrayF, NDim1 & _is_zero_or_finite_and_non_micro_or_nan
]
NDArrayF1ZrNan = Annotated[NDArrayF, NDim1 & _is_zero_or_nan]
NDArrayF1ZrNonMic = Annotated[NDArrayF, NDim1 & _is_zero_or_non_micro]
NDArrayF1ZrNonMicNan = Annotated[NDArrayF, NDim1 & _is_zero_or_non_micro_or_nan]

NDArrayF2Fin = Annotated[NDArrayF, NDim2 & _is_finite]
NDArrayF2FinInt = Annotated[NDArrayF, NDim2 & _is_finite_and_integral]
NDArrayF2FinIntNan = Annotated[NDArrayF, NDim2 & _is_finite_and_integral_or_nan]
NDArrayF2FinNeg = Annotated[NDArrayF, NDim2 & _is_finite_and_negative]
NDArrayF2FinNegNan = Annotated[NDArrayF, NDim2 & _is_finite_and_negative_or_nan]
NDArrayF2FinNonNeg = Annotated[NDArrayF, NDim2 & _is_finite_and_non_negative]
NDArrayF2FinNonNegNan = Annotated[
    NDArrayF, NDim2 & _is_finite_and_non_negative_or_nan
]
NDArrayF2FinNonPos = Annotated[NDArrayF, NDim2 & _is_finite_and_non_positive]
NDArrayF2FinNonPosNan = Annotated[
    NDArrayF, NDim2 & _is_finite_and_non_positive_or_nan
]
NDArrayF2FinNonZr = Annotated[NDArrayF, NDim2 & _is_finite_and_non_zero]
NDArrayF2FinNonZrNan = Annotated[
    NDArrayF, NDim2 & _is_finite_and_non_zero_or_nan
]
NDArrayF2FinPos = Annotated[NDArrayF, NDim2 & _is_finite_and_positive]
NDArrayF2FinPosNan = Annotated[NDArrayF, NDim2 & _is_finite_and_positive_or_nan]
NDArrayF2FinNan = Annotated[NDArrayF, NDim2 & _is_finite_or_nan]
NDArrayF2Int = Annotated[NDArrayF, NDim2 & _is_integral]
NDArrayF2IntNan = Annotated[NDArrayF, NDim2 & _is_integral_or_nan]
NDArrayF2Neg = Annotated[NDArrayF, NDim2 & _is_negative]
NDArrayF2NegNan = Annotated[NDArrayF, NDim2 & _is_negative_or_nan]
NDArrayF2NonNeg = Annotated[NDArrayF, NDim2 & _is_non_negative]
NDArrayF2NonNegNan = Annotated[NDArrayF, NDim2 & _is_non_negative_or_nan]
NDArrayF2NonPos = Annotated[NDArrayF, NDim2 & _is_non_positive]
NDArrayF2NonPosNan = Annotated[NDArrayF, NDim2 & _is_non_positive_or_nan]
NDArrayF2NonZr = Annotated[NDArrayF, NDim2 & _is_non_zero]
NDArrayF2NonZrNan = Annotated[NDArrayF, NDim2 & _is_non_zero_or_nan]
NDArrayF2Pos = Annotated[NDArrayF, NDim2 & _is_positive]
NDArrayF2PosNan = Annotated[NDArrayF, NDim2 & _is_positive_or_nan]
NDArrayF2Zr = Annotated[NDArrayF, NDim2 & _is_zero]
NDArrayF2ZrFinNonMic = Annotated[
    NDArrayF, NDim2 & _is_zero_or_finite_and_non_micro
]
NDArrayF2ZrFinNonMicNan = Annotated[
    NDArrayF, NDim2 & _is_zero_or_finite_and_non_micro_or_nan
]
NDArrayF2ZrNan = Annotated[NDArrayF, NDim2 & _is_zero_or_nan]
NDArrayF2ZrNonMic = Annotated[NDArrayF, NDim2 & _is_zero_or_non_micro]
NDArrayF2ZrNonMicNan = Annotated[NDArrayF, NDim2 & _is_zero_or_non_micro_or_nan]

NDArrayF3Fin = Annotated[NDArrayF, NDim3 & _is_finite]
NDArrayF3FinInt = Annotated[NDArrayF, NDim3 & _is_finite_and_integral]
NDArrayF3FinIntNan = Annotated[NDArrayF, NDim3 & _is_finite_and_integral_or_nan]
NDArrayF3FinNeg = Annotated[NDArrayF, NDim3 & _is_finite_and_negative]
NDArrayF3FinNegNan = Annotated[NDArrayF, NDim3 & _is_finite_and_negative_or_nan]
NDArrayF3FinNonNeg = Annotated[NDArrayF, NDim3 & _is_finite_and_non_negative]
NDArrayF3FinNonNegNan = Annotated[
    NDArrayF, NDim3 & _is_finite_and_non_negative_or_nan
]
NDArrayF3FinNonPos = Annotated[NDArrayF, NDim3 & _is_finite_and_non_positive]
NDArrayF3FinNonPosNan = Annotated[
    NDArrayF, NDim3 & _is_finite_and_non_positive_or_nan
]
NDArrayF3FinNonZr = Annotated[NDArrayF, NDim3 & _is_finite_and_non_zero]
NDArrayF3FinNonZrNan = Annotated[
    NDArrayF, NDim3 & _is_finite_and_non_zero_or_nan
]
NDArrayF3FinPos = Annotated[NDArrayF, NDim3 & _is_finite_and_positive]
NDArrayF3FinPosNan = Annotated[NDArrayF, NDim3 & _is_finite_and_positive_or_nan]
NDArrayF3FinNan = Annotated[NDArrayF, NDim3 & _is_finite_or_nan]
NDArrayF3Int = Annotated[NDArrayF, NDim3 & _is_integral]
NDArrayF3IntNan = Annotated[NDArrayF, NDim3 & _is_integral_or_nan]
NDArrayF3Neg = Annotated[NDArrayF, NDim3 & _is_negative]
NDArrayF3NegNan = Annotated[NDArrayF, NDim3 & _is_negative_or_nan]
NDArrayF3NonNeg = Annotated[NDArrayF, NDim3 & _is_non_negative]
NDArrayF3NonNegNan = Annotated[NDArrayF, NDim3 & _is_non_negative_or_nan]
NDArrayF3NonPos = Annotated[NDArrayF, NDim3 & _is_non_positive]
NDArrayF3NonPosNan = Annotated[NDArrayF, NDim3 & _is_non_positive_or_nan]
NDArrayF3NonZr = Annotated[NDArrayF, NDim3 & _is_non_zero]
NDArrayF3NonZrNan = Annotated[NDArrayF, NDim3 & _is_non_zero_or_nan]
NDArrayF3Pos = Annotated[NDArrayF, NDim3 & _is_positive]
NDArrayF3PosNan = Annotated[NDArrayF, NDim3 & _is_positive_or_nan]
NDArrayF3Zr = Annotated[NDArrayF, NDim3 & _is_zero]
NDArrayF3ZrFinNonMic = Annotated[
    NDArrayF, NDim3 & _is_zero_or_finite_and_non_micro
]
NDArrayF3ZrFinNonMicNan = Annotated[
    NDArrayF, NDim3 & _is_zero_or_finite_and_non_micro_or_nan
]
NDArrayF3ZrNan = Annotated[NDArrayF, NDim3 & _is_zero_or_nan]
NDArrayF3ZrNonMic = Annotated[NDArrayF, NDim3 & _is_zero_or_non_micro]
NDArrayF3ZrNonMicNan = Annotated[NDArrayF, NDim3 & _is_zero_or_non_micro_or_nan]
