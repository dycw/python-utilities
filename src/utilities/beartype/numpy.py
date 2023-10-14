from __future__ import annotations

from typing import Annotated
from typing import Any
from typing import cast
from typing import get_args

from beartype.vale import Is
from beartype.vale import IsAttr
from beartype.vale import IsEqual
from numpy import log
from numpy import unravel_index
from numpy.random import default_rng

import utilities.numpy
from utilities.numpy import HasNDim
from utilities.numpy import HasPredicate
from utilities.numpy import NDArrayF
from utilities.numpy import NDArrayI


# has ndim
def _convert_has_ndim(ann: HasNDim, /) -> Any:
    return IsAttr["ndim", IsEqual[ann.ndim]]


NDim0 = _convert_has_ndim(utilities.numpy.NDim0)
NDim1 = _convert_has_ndim(utilities.numpy.NDim1)
NDim2 = _convert_has_ndim(utilities.numpy.NDim2)
NDim3 = _convert_has_ndim(utilities.numpy.NDim3)


# has predicate
def _convert_has_predicate(ann: HasPredicate, /) -> Any:
    """Apply the predicate to a subset of a float array."""
    predicate = ann.predicate
    rng = default_rng()

    def inner_predicate(array: NDArrayI | NDArrayF, /) -> bool:
        if (size := array.size) == 0:
            return True
        if size == 1:
            return predicate(array).item()
        num_samples = round(log(size))
        indices = rng.integers(0, size, size=num_samples)
        sample = array[unravel_index(indices, array.shape)]
        return predicate(sample).all().item()

    return Is[cast(Any, inner_predicate)]


IsFinite = _convert_has_predicate(utilities.numpy.IsFinite)
IsFiniteAndIntegral = _convert_has_predicate(
    utilities.numpy.IsFiniteAndIntegral
)
IsFiniteAndIntegralOrNan = _convert_has_predicate(
    utilities.numpy.IsFiniteAndIntegralOrNan
)
IsFiniteAndNegative = _convert_has_predicate(
    utilities.numpy.IsFiniteAndNegative
)
IsFiniteAndNegativeOrNan = _convert_has_predicate(
    utilities.numpy.IsFiniteAndNegativeOrNan
)
IsFiniteAndNonNegative = _convert_has_predicate(
    utilities.numpy.IsFiniteAndNonNegative
)
IsFiniteAndNonNegativeOrNan = _convert_has_predicate(
    utilities.numpy.IsFiniteAndNonNegativeOrNan
)
IsFiniteAndNonPositive = _convert_has_predicate(
    utilities.numpy.IsFiniteAndNonPositive
)
IsFiniteAndNonPositiveOrNan = _convert_has_predicate(
    utilities.numpy.IsFiniteAndNonPositiveOrNan
)
IsFiniteAndNonZero = _convert_has_predicate(utilities.numpy.IsFiniteAndNonZero)
IsFiniteAndNonZeroOrNan = _convert_has_predicate(
    utilities.numpy.IsFiniteAndNonZeroOrNan
)
IsFiniteAndPositive = _convert_has_predicate(
    utilities.numpy.IsFiniteAndPositive
)
IsFiniteAndPositiveOrNan = _convert_has_predicate(
    utilities.numpy.IsFiniteAndPositiveOrNan
)
IsFiniteOrNan = _convert_has_predicate(utilities.numpy.IsFiniteOrNan)
IsIntegral = _convert_has_predicate(utilities.numpy.IsIntegral)
IsIntegralOrNan = _convert_has_predicate(utilities.numpy.IsIntegralOrNan)
IsNegative = _convert_has_predicate(utilities.numpy.IsNegative)
IsNegativeOrNan = _convert_has_predicate(utilities.numpy.IsNegativeOrNan)
IsNonNegative = _convert_has_predicate(utilities.numpy.IsNonNegative)
IsNonNegativeOrNan = _convert_has_predicate(utilities.numpy.IsNonNegativeOrNan)
IsNonPositive = _convert_has_predicate(utilities.numpy.IsNonPositive)
IsNonPositiveOrNan = _convert_has_predicate(utilities.numpy.IsNonPositiveOrNan)
IsNonZero = _convert_has_predicate(utilities.numpy.IsNonZero)
IsNonZeroOrNan = _convert_has_predicate(utilities.numpy.IsNonZeroOrNan)
IsPositive = _convert_has_predicate(utilities.numpy.IsPositive)
IsPositiveOrNan = _convert_has_predicate(utilities.numpy.IsPositiveOrNan)
IsZero = _convert_has_predicate(utilities.numpy.IsZero)
IsZeroOrFiniteAndNonMicro = _convert_has_predicate(
    utilities.numpy.IsZeroOrFiniteAndNonMicro
)
IsZeroOrFiniteAndNonMicroOrNan = _convert_has_predicate(
    utilities.numpy.IsZeroOrFiniteAndNonMicroOrNan
)
IsZeroOrNan = _convert_has_predicate(utilities.numpy.IsZeroOrNan)
IsZeroOrNonMicro = _convert_has_predicate(utilities.numpy.IsZeroOrNonMicro)
IsZeroOrNonMicroOrNan = _convert_has_predicate(
    utilities.numpy.IsZeroOrNonMicroOrNan
)


# annotations - ndims


def _convert_arg(arg: HasNDim | HasPredicate, /) -> Any:
    if isinstance(arg, HasNDim):
        return _convert_has_ndim(arg)
    return _convert_has_predicate(arg)


def _convert_annotated(arg: Any, /) -> Any:
    try:
        cls, ann = get_args(arg)
    except ValueError:
        cls, ann1, ann2 = get_args(arg)
        return Annotated[cls, _convert_arg(ann1), _convert_arg(ann2)]
    else:
        return Annotated[cls, _convert_arg(ann)]


NDArray0 = _convert_annotated(utilities.numpy.NDArray0)
NDArray1 = _convert_annotated(utilities.numpy.NDArray1)
NDArray2 = _convert_annotated(utilities.numpy.NDArray2)
NDArray3 = _convert_annotated(utilities.numpy.NDArray3)


# annotations - dtype & ndim
NDArrayB0 = _convert_annotated(utilities.numpy.NDArrayB0)
NDArrayD1 = _convert_annotated(utilities.numpy.NDArrayD1)
NDArrayD1 = _convert_annotated(utilities.numpy.NDArrayD1)
NDArrayDY0 = _convert_annotated(utilities.numpy.NDArrayDY0)
NDArrayDM0 = _convert_annotated(utilities.numpy.NDArrayDM0)
NDArrayDW0 = _convert_annotated(utilities.numpy.NDArrayDW0)
NDArrayDD0 = _convert_annotated(utilities.numpy.NDArrayDD0)
NDArrayDh0 = _convert_annotated(utilities.numpy.NDArrayDh0)
NDArrayDm0 = _convert_annotated(utilities.numpy.NDArrayDm0)
NDArrayDs0 = _convert_annotated(utilities.numpy.NDArrayDs0)
NDArrayDms0 = _convert_annotated(utilities.numpy.NDArrayDms0)
NDArrayDus0 = _convert_annotated(utilities.numpy.NDArrayDus0)
NDArrayDns0 = _convert_annotated(utilities.numpy.NDArrayDns0)
NDArrayDps0 = _convert_annotated(utilities.numpy.NDArrayDps0)
NDArrayDfs0 = _convert_annotated(utilities.numpy.NDArrayDfs0)
NDArrayDas0 = _convert_annotated(utilities.numpy.NDArrayDas0)
NDArrayF0 = _convert_annotated(utilities.numpy.NDArrayF0)
NDArrayI0 = _convert_annotated(utilities.numpy.NDArrayI0)
NDArrayO0 = _convert_annotated(utilities.numpy.NDArrayO0)

NDArrayB1 = _convert_annotated(utilities.numpy.NDArrayB1)
NDArrayD1 = _convert_annotated(utilities.numpy.NDArrayD1)
NDArrayDY1 = _convert_annotated(utilities.numpy.NDArrayDY1)
NDArrayDM1 = _convert_annotated(utilities.numpy.NDArrayDM1)
NDArrayDW1 = _convert_annotated(utilities.numpy.NDArrayDW1)
NDArrayDD1 = _convert_annotated(utilities.numpy.NDArrayDD1)
NDArrayDh1 = _convert_annotated(utilities.numpy.NDArrayDh1)
NDArrayDm1 = _convert_annotated(utilities.numpy.NDArrayDm1)
NDArrayDs1 = _convert_annotated(utilities.numpy.NDArrayDs1)
NDArrayDms1 = _convert_annotated(utilities.numpy.NDArrayDms1)
NDArrayDus1 = _convert_annotated(utilities.numpy.NDArrayDus1)
NDArrayDns1 = _convert_annotated(utilities.numpy.NDArrayDns1)
NDArrayDps1 = _convert_annotated(utilities.numpy.NDArrayDps1)
NDArrayDfs1 = _convert_annotated(utilities.numpy.NDArrayDfs1)
NDArrayDas1 = _convert_annotated(utilities.numpy.NDArrayDas1)
NDArrayF1 = _convert_annotated(utilities.numpy.NDArrayF1)
NDArrayI1 = _convert_annotated(utilities.numpy.NDArrayI1)
NDArrayO1 = _convert_annotated(utilities.numpy.NDArrayO1)

NDArrayB2 = _convert_annotated(utilities.numpy.NDArrayB2)
NDArrayD2 = _convert_annotated(utilities.numpy.NDArrayD2)
NDArrayDY2 = _convert_annotated(utilities.numpy.NDArrayDY2)
NDArrayDM2 = _convert_annotated(utilities.numpy.NDArrayDM2)
NDArrayDW2 = _convert_annotated(utilities.numpy.NDArrayDW2)
NDArrayDD2 = _convert_annotated(utilities.numpy.NDArrayDD2)
NDArrayDh2 = _convert_annotated(utilities.numpy.NDArrayDh2)
NDArrayDm2 = _convert_annotated(utilities.numpy.NDArrayDm2)
NDArrayDs2 = _convert_annotated(utilities.numpy.NDArrayDs2)
NDArrayDms2 = _convert_annotated(utilities.numpy.NDArrayDms2)
NDArrayDus2 = _convert_annotated(utilities.numpy.NDArrayDus2)
NDArrayDns2 = _convert_annotated(utilities.numpy.NDArrayDns2)
NDArrayDps2 = _convert_annotated(utilities.numpy.NDArrayDps2)
NDArrayDfs2 = _convert_annotated(utilities.numpy.NDArrayDfs2)
NDArrayDas2 = _convert_annotated(utilities.numpy.NDArrayDas2)
NDArrayF2 = _convert_annotated(utilities.numpy.NDArrayF2)
NDArrayI2 = _convert_annotated(utilities.numpy.NDArrayI2)
NDArrayO2 = _convert_annotated(utilities.numpy.NDArrayO2)

NDArrayB3 = _convert_annotated(utilities.numpy.NDArrayB3)
NDArrayD3 = _convert_annotated(utilities.numpy.NDArrayD3)
NDArrayDY3 = _convert_annotated(utilities.numpy.NDArrayDY3)
NDArrayDM3 = _convert_annotated(utilities.numpy.NDArrayDM3)
NDArrayDW3 = _convert_annotated(utilities.numpy.NDArrayDW3)
NDArrayDD3 = _convert_annotated(utilities.numpy.NDArrayDD3)
NDArrayDh3 = _convert_annotated(utilities.numpy.NDArrayDh3)
NDArrayDm3 = _convert_annotated(utilities.numpy.NDArrayDm3)
NDArrayDs3 = _convert_annotated(utilities.numpy.NDArrayDs3)
NDArrayDms3 = _convert_annotated(utilities.numpy.NDArrayDms3)
NDArrayDus3 = _convert_annotated(utilities.numpy.NDArrayDus3)
NDArrayDns3 = _convert_annotated(utilities.numpy.NDArrayDns3)
NDArrayDps3 = _convert_annotated(utilities.numpy.NDArrayDps3)
NDArrayDfs3 = _convert_annotated(utilities.numpy.NDArrayDfs3)
NDArrayDas3 = _convert_annotated(utilities.numpy.NDArrayDas3)
NDArrayF3 = _convert_annotated(utilities.numpy.NDArrayF3)
NDArrayI3 = _convert_annotated(utilities.numpy.NDArrayI3)
NDArrayO3 = _convert_annotated(utilities.numpy.NDArrayO3)


# annotations - int & predicates
NDArrayINeg = _convert_annotated(utilities.numpy.NDArrayINeg)
NDArrayINonNeg = _convert_annotated(utilities.numpy.NDArrayINonNeg)
NDArrayINonPos = _convert_annotated(utilities.numpy.NDArrayINonPos)
NDArrayINonZr = _convert_annotated(utilities.numpy.NDArrayINonZr)
NDArrayIPos = _convert_annotated(utilities.numpy.NDArrayIPos)
NDArrayIZr = _convert_annotated(utilities.numpy.NDArrayIZr)


# annotations - float & predicates
NDArrayFFin = _convert_annotated(utilities.numpy.NDArrayFFin)
NDArrayFFinInt = _convert_annotated(utilities.numpy.NDArrayFFinInt)
NDArrayFFinIntNan = _convert_annotated(utilities.numpy.NDArrayFFinIntNan)
NDArrayFFinNeg = _convert_annotated(utilities.numpy.NDArrayFFinNeg)
NDArrayFFinNegNan = _convert_annotated(utilities.numpy.NDArrayFFinNegNan)
NDArrayFFinNonNeg = _convert_annotated(utilities.numpy.NDArrayFFinNonNeg)
NDArrayFFinNonNegNan = _convert_annotated(utilities.numpy.NDArrayFFinNonNegNan)
NDArrayFFinNonPos = _convert_annotated(utilities.numpy.NDArrayFFinNonPos)
NDArrayFFinNonPosNan = _convert_annotated(utilities.numpy.NDArrayFFinNonPosNan)
NDArrayFFinNonZr = _convert_annotated(utilities.numpy.NDArrayFFinNonZr)
NDArrayFFinNonZrNan = _convert_annotated(utilities.numpy.NDArrayFFinNonZrNan)
NDArrayFFinPos = _convert_annotated(utilities.numpy.NDArrayFFinPos)
NDArrayFFinPosNan = _convert_annotated(utilities.numpy.NDArrayFFinPosNan)
NDArrayFFinNan = _convert_annotated(utilities.numpy.NDArrayFFinNan)
NDArrayFInt = _convert_annotated(utilities.numpy.NDArrayFInt)
NDArrayFIntNan = _convert_annotated(utilities.numpy.NDArrayFIntNan)
NDArrayFNeg = _convert_annotated(utilities.numpy.NDArrayFNeg)
NDArrayFNegNan = _convert_annotated(utilities.numpy.NDArrayFNegNan)
NDArrayFNonNeg = _convert_annotated(utilities.numpy.NDArrayFNonNeg)
NDArrayFNonNegNan = _convert_annotated(utilities.numpy.NDArrayFNonNegNan)
NDArrayFNonPos = _convert_annotated(utilities.numpy.NDArrayFNonPos)
NDArrayFNonPosNan = _convert_annotated(utilities.numpy.NDArrayFNonPosNan)
NDArrayFNonZr = _convert_annotated(utilities.numpy.NDArrayFNonZr)
NDArrayFNonZrNan = _convert_annotated(utilities.numpy.NDArrayFNonZrNan)
NDArrayFPos = _convert_annotated(utilities.numpy.NDArrayFPos)
NDArrayFPosNan = _convert_annotated(utilities.numpy.NDArrayFPosNan)
NDArrayFZr = _convert_annotated(utilities.numpy.NDArrayFZr)
NDArrayFZrFinNonMic = _convert_annotated(utilities.numpy.NDArrayFZrFinNonMic)
NDArrayFZrFinNonMicNan = _convert_annotated(
    utilities.numpy.NDArrayFZrFinNonMicNan
)
NDArrayFZrNan = _convert_annotated(utilities.numpy.NDArrayFZrNan)
NDArrayFZrNonMic = _convert_annotated(utilities.numpy.NDArrayFZrNonMic)
NDArrayFZrNonMicNan = _convert_annotated(utilities.numpy.NDArrayFZrNonMicNan)


# annotations - int, ndim & predicate
NDArrayI0Neg = _convert_annotated(utilities.numpy.NDArrayI0Neg)
NDArrayI0NonNeg = _convert_annotated(utilities.numpy.NDArrayI0NonNeg)
NDArrayI0NonPos = _convert_annotated(utilities.numpy.NDArrayI0NonPos)
NDArrayI0NonZr = _convert_annotated(utilities.numpy.NDArrayI0NonZr)
NDArrayI0Pos = _convert_annotated(utilities.numpy.NDArrayI0Pos)
NDArrayI0Zr = _convert_annotated(utilities.numpy.NDArrayI0Zr)

NDArrayI1Neg = _convert_annotated(utilities.numpy.NDArrayI1Neg)
NDArrayI1NonNeg = _convert_annotated(utilities.numpy.NDArrayI1NonNeg)
NDArrayI1NonPos = _convert_annotated(utilities.numpy.NDArrayI1NonPos)
NDArrayI1NonZr = _convert_annotated(utilities.numpy.NDArrayI1NonZr)
NDArrayI1Pos = _convert_annotated(utilities.numpy.NDArrayI1Pos)
NDArrayI1Zr = _convert_annotated(utilities.numpy.NDArrayI1Zr)

NDArrayI2Neg = _convert_annotated(utilities.numpy.NDArrayI2Neg)
NDArrayI2NonNeg = _convert_annotated(utilities.numpy.NDArrayI2NonNeg)
NDArrayI2NonPos = _convert_annotated(utilities.numpy.NDArrayI2NonPos)
NDArrayI2NonZr = _convert_annotated(utilities.numpy.NDArrayI2NonZr)
NDArrayI2Pos = _convert_annotated(utilities.numpy.NDArrayI2Pos)
NDArrayI2Zr = _convert_annotated(utilities.numpy.NDArrayI2Zr)

NDArrayI3Neg = _convert_annotated(utilities.numpy.NDArrayI3Neg)
NDArrayI3NonNeg = _convert_annotated(utilities.numpy.NDArrayI3NonNeg)
NDArrayI3NonPos = _convert_annotated(utilities.numpy.NDArrayI3NonPos)
NDArrayI3NonZr = _convert_annotated(utilities.numpy.NDArrayI3NonZr)
NDArrayI3Pos = _convert_annotated(utilities.numpy.NDArrayI3Pos)
NDArrayI3Zr = _convert_annotated(utilities.numpy.NDArrayI3Zr)


# annotations - float, ndim & predicate
NDArrayF0Fin = _convert_annotated(utilities.numpy.NDArrayF0Fin)
NDArrayF0FinInt = _convert_annotated(utilities.numpy.NDArrayF0FinInt)
NDArrayF0FinIntNan = _convert_annotated(utilities.numpy.NDArrayF0FinIntNan)
NDArrayF0FinNeg = _convert_annotated(utilities.numpy.NDArrayF0FinNeg)
NDArrayF0FinNegNan = _convert_annotated(utilities.numpy.NDArrayF0FinNegNan)
NDArrayF0FinNonNeg = _convert_annotated(utilities.numpy.NDArrayF0FinNonNeg)
NDArrayF0FinNonNegNan = _convert_annotated(
    utilities.numpy.NDArrayF0FinNonNegNan
)
NDArrayF0FinNonPos = _convert_annotated(utilities.numpy.NDArrayF0FinNonPos)
NDArrayF0FinNonPosNan = _convert_annotated(
    utilities.numpy.NDArrayF0FinNonPosNan
)
NDArrayF0FinNonZr = _convert_annotated(utilities.numpy.NDArrayF0FinNonZr)
NDArrayF0FinNonZrNan = _convert_annotated(utilities.numpy.NDArrayF0FinNonZrNan)
NDArrayF0FinPos = _convert_annotated(utilities.numpy.NDArrayF0FinPos)
NDArrayF0FinPosNan = _convert_annotated(utilities.numpy.NDArrayF0FinPosNan)
NDArrayF0FinNan = _convert_annotated(utilities.numpy.NDArrayF0FinNan)
NDArrayF0Int = _convert_annotated(utilities.numpy.NDArrayF0Int)
NDArrayF0IntNan = _convert_annotated(utilities.numpy.NDArrayF0IntNan)
NDArrayF0Neg = _convert_annotated(utilities.numpy.NDArrayF0Neg)
NDArrayF0NegNan = _convert_annotated(utilities.numpy.NDArrayF0NegNan)
NDArrayF0NonNeg = _convert_annotated(utilities.numpy.NDArrayF0NonNeg)
NDArrayF0NonNegNan = _convert_annotated(utilities.numpy.NDArrayF0NonNegNan)
NDArrayF0NonPos = _convert_annotated(utilities.numpy.NDArrayF0NonPos)
NDArrayF0NonPosNan = _convert_annotated(utilities.numpy.NDArrayF0NonPosNan)
NDArrayF0NonZr = _convert_annotated(utilities.numpy.NDArrayF0NonZr)
NDArrayF0NonZrNan = _convert_annotated(utilities.numpy.NDArrayF0NonZrNan)
NDArrayF0Pos = _convert_annotated(utilities.numpy.NDArrayF0Pos)
NDArrayF0PosNan = _convert_annotated(utilities.numpy.NDArrayF0PosNan)
NDArrayF0Zr = _convert_annotated(utilities.numpy.NDArrayF0Zr)
NDArrayF0ZrFinNonMic = _convert_annotated(utilities.numpy.NDArrayF0ZrFinNonMic)
NDArrayF0ZrFinNonMicNan = _convert_annotated(
    utilities.numpy.NDArrayF0ZrFinNonMicNan
)
NDArrayF0ZrNan = _convert_annotated(utilities.numpy.NDArrayF0ZrNan)
NDArrayF0ZrNonMic = _convert_annotated(utilities.numpy.NDArrayF0ZrNonMic)
NDArrayF0ZrNonMicNan = _convert_annotated(utilities.numpy.NDArrayF0ZrNonMicNan)

NDArrayF1Fin = _convert_annotated(utilities.numpy.NDArrayF1Fin)
NDArrayF1FinInt = _convert_annotated(utilities.numpy.NDArrayF1FinInt)
NDArrayF1FinIntNan = _convert_annotated(utilities.numpy.NDArrayF1FinIntNan)
NDArrayF1FinNeg = _convert_annotated(utilities.numpy.NDArrayF1FinNeg)
NDArrayF1FinNegNan = _convert_annotated(utilities.numpy.NDArrayF1FinNegNan)
NDArrayF1FinNonNeg = _convert_annotated(utilities.numpy.NDArrayF1FinNonNeg)
NDArrayF1FinNonNegNan = _convert_annotated(
    utilities.numpy.NDArrayF1FinNonNegNan
)
NDArrayF1FinNonPos = _convert_annotated(utilities.numpy.NDArrayF1FinNonPos)
NDArrayF1FinNonPosNan = _convert_annotated(
    utilities.numpy.NDArrayF1FinNonPosNan
)
NDArrayF1FinNonZr = _convert_annotated(utilities.numpy.NDArrayF1FinNonZr)
NDArrayF1FinNonZrNan = _convert_annotated(utilities.numpy.NDArrayF1FinNonZrNan)
NDArrayF1FinPos = _convert_annotated(utilities.numpy.NDArrayF1FinPos)
NDArrayF1FinPosNan = _convert_annotated(utilities.numpy.NDArrayF1FinPosNan)
NDArrayF1FinNan = _convert_annotated(utilities.numpy.NDArrayF1FinNan)
NDArrayF1Int = _convert_annotated(utilities.numpy.NDArrayF1Int)
NDArrayF1IntNan = _convert_annotated(utilities.numpy.NDArrayF1IntNan)
NDArrayF1Neg = _convert_annotated(utilities.numpy.NDArrayF1Neg)
NDArrayF1NegNan = _convert_annotated(utilities.numpy.NDArrayF1NegNan)
NDArrayF1NonNeg = _convert_annotated(utilities.numpy.NDArrayF1NonNeg)
NDArrayF1NonNegNan = _convert_annotated(utilities.numpy.NDArrayF1NonNegNan)
NDArrayF1NonPos = _convert_annotated(utilities.numpy.NDArrayF1NonPos)
NDArrayF1NonPosNan = _convert_annotated(utilities.numpy.NDArrayF1NonPosNan)
NDArrayF1NonZr = _convert_annotated(utilities.numpy.NDArrayF1NonZr)
NDArrayF1NonZrNan = _convert_annotated(utilities.numpy.NDArrayF1NonZrNan)
NDArrayF1Pos = _convert_annotated(utilities.numpy.NDArrayF1Pos)
NDArrayF1PosNan = _convert_annotated(utilities.numpy.NDArrayF1PosNan)
NDArrayF1Zr = _convert_annotated(utilities.numpy.NDArrayF1Zr)
NDArrayF1ZrFinNonMic = _convert_annotated(utilities.numpy.NDArrayF1ZrFinNonMic)
NDArrayF1ZrFinNonMicNan = _convert_annotated(
    utilities.numpy.NDArrayF1ZrFinNonMicNan
)
NDArrayF1ZrNan = _convert_annotated(utilities.numpy.NDArrayF1ZrNan)
NDArrayF1ZrNonMic = _convert_annotated(utilities.numpy.NDArrayF1ZrNonMic)
NDArrayF1ZrNonMicNan = _convert_annotated(utilities.numpy.NDArrayF1ZrNonMicNan)

NDArrayF2Fin = _convert_annotated(utilities.numpy.NDArrayF2Fin)
NDArrayF2FinInt = _convert_annotated(utilities.numpy.NDArrayF2FinInt)
NDArrayF2FinIntNan = _convert_annotated(utilities.numpy.NDArrayF2FinIntNan)
NDArrayF2FinNeg = _convert_annotated(utilities.numpy.NDArrayF2FinNeg)
NDArrayF2FinNegNan = _convert_annotated(utilities.numpy.NDArrayF2FinNegNan)
NDArrayF2FinNonNeg = _convert_annotated(utilities.numpy.NDArrayF2FinNonNeg)
NDArrayF2FinNonNegNan = _convert_annotated(
    utilities.numpy.NDArrayF2FinNonNegNan
)
NDArrayF2FinNonPos = _convert_annotated(utilities.numpy.NDArrayF2FinNonPos)
NDArrayF2FinNonPosNan = _convert_annotated(
    utilities.numpy.NDArrayF2FinNonPosNan
)
NDArrayF2FinNonZr = _convert_annotated(utilities.numpy.NDArrayF2FinNonZr)
NDArrayF2FinNonZrNan = _convert_annotated(utilities.numpy.NDArrayF2FinNonZrNan)
NDArrayF2FinPos = _convert_annotated(utilities.numpy.NDArrayF2FinPos)
NDArrayF2FinPosNan = _convert_annotated(utilities.numpy.NDArrayF2FinPosNan)
NDArrayF2FinNan = _convert_annotated(utilities.numpy.NDArrayF2FinNan)
NDArrayF2Int = _convert_annotated(utilities.numpy.NDArrayF2Int)
NDArrayF2IntNan = _convert_annotated(utilities.numpy.NDArrayF2IntNan)
NDArrayF2Neg = _convert_annotated(utilities.numpy.NDArrayF2Neg)
NDArrayF2NegNan = _convert_annotated(utilities.numpy.NDArrayF2NegNan)
NDArrayF2NonNeg = _convert_annotated(utilities.numpy.NDArrayF2NonNeg)
NDArrayF2NonNegNan = _convert_annotated(utilities.numpy.NDArrayF2NonNegNan)
NDArrayF2NonPos = _convert_annotated(utilities.numpy.NDArrayF2NonPos)
NDArrayF2NonPosNan = _convert_annotated(utilities.numpy.NDArrayF2NonPosNan)
NDArrayF2NonZr = _convert_annotated(utilities.numpy.NDArrayF2NonZr)
NDArrayF2NonZrNan = _convert_annotated(utilities.numpy.NDArrayF2NonZrNan)
NDArrayF2Pos = _convert_annotated(utilities.numpy.NDArrayF2Pos)
NDArrayF2PosNan = _convert_annotated(utilities.numpy.NDArrayF2PosNan)
NDArrayF2Zr = _convert_annotated(utilities.numpy.NDArrayF2Zr)
NDArrayF2ZrFinNonMic = _convert_annotated(utilities.numpy.NDArrayF2ZrFinNonMic)
NDArrayF2ZrFinNonMicNan = _convert_annotated(
    utilities.numpy.NDArrayF2ZrFinNonMicNan
)
NDArrayF2ZrNan = _convert_annotated(utilities.numpy.NDArrayF2ZrNan)
NDArrayF2ZrNonMic = _convert_annotated(utilities.numpy.NDArrayF2ZrNonMic)
NDArrayF2ZrNonMicNan = _convert_annotated(utilities.numpy.NDArrayF2ZrNonMicNan)

NDArrayF3Fin = _convert_annotated(utilities.numpy.NDArrayF3Fin)
NDArrayF3FinInt = _convert_annotated(utilities.numpy.NDArrayF3FinInt)
NDArrayF3FinIntNan = _convert_annotated(utilities.numpy.NDArrayF3FinIntNan)
NDArrayF3FinNeg = _convert_annotated(utilities.numpy.NDArrayF3FinNeg)
NDArrayF3FinNegNan = _convert_annotated(utilities.numpy.NDArrayF3FinNegNan)
NDArrayF3FinNonNeg = _convert_annotated(utilities.numpy.NDArrayF3FinNonNeg)
NDArrayF3FinNonNegNan = _convert_annotated(
    utilities.numpy.NDArrayF3FinNonNegNan
)
NDArrayF3FinNonPos = _convert_annotated(utilities.numpy.NDArrayF3FinNonPos)
NDArrayF3FinNonPosNan = _convert_annotated(
    utilities.numpy.NDArrayF3FinNonPosNan
)
NDArrayF3FinNonZr = _convert_annotated(utilities.numpy.NDArrayF3FinNonZr)
NDArrayF3FinNonZrNan = _convert_annotated(utilities.numpy.NDArrayF3FinNonZrNan)
NDArrayF3FinPos = _convert_annotated(utilities.numpy.NDArrayF3FinPos)
NDArrayF3FinPosNan = _convert_annotated(utilities.numpy.NDArrayF3FinPosNan)
NDArrayF3FinNan = _convert_annotated(utilities.numpy.NDArrayF3FinNan)
NDArrayF3Int = _convert_annotated(utilities.numpy.NDArrayF3Int)
NDArrayF3IntNan = _convert_annotated(utilities.numpy.NDArrayF3IntNan)
NDArrayF3Neg = _convert_annotated(utilities.numpy.NDArrayF3Neg)
NDArrayF3NegNan = _convert_annotated(utilities.numpy.NDArrayF3NegNan)
NDArrayF3NonNeg = _convert_annotated(utilities.numpy.NDArrayF3NonNeg)
NDArrayF3NonNegNan = _convert_annotated(utilities.numpy.NDArrayF3NonNegNan)
NDArrayF3NonPos = _convert_annotated(utilities.numpy.NDArrayF3NonPos)
NDArrayF3NonPosNan = _convert_annotated(utilities.numpy.NDArrayF3NonPosNan)
NDArrayF3NonZr = _convert_annotated(utilities.numpy.NDArrayF3NonZr)
NDArrayF3NonZrNan = _convert_annotated(utilities.numpy.NDArrayF3NonZrNan)
NDArrayF3Pos = _convert_annotated(utilities.numpy.NDArrayF3Pos)
NDArrayF3PosNan = _convert_annotated(utilities.numpy.NDArrayF3PosNan)
NDArrayF3Zr = _convert_annotated(utilities.numpy.NDArrayF3Zr)
NDArrayF3ZrFinNonMic = _convert_annotated(utilities.numpy.NDArrayF3ZrFinNonMic)
NDArrayF3ZrFinNonMicNan = _convert_annotated(
    utilities.numpy.NDArrayF3ZrFinNonMicNan
)
NDArrayF3ZrNan = _convert_annotated(utilities.numpy.NDArrayF3ZrNan)
NDArrayF3ZrNonMic = _convert_annotated(utilities.numpy.NDArrayF3ZrNonMic)
NDArrayF3ZrNonMicNan = _convert_annotated(utilities.numpy.NDArrayF3ZrNonMicNan)
